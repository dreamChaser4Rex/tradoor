//! The server over HTTP, the way a browser meets it: the real binary is started
//! against a Postgres database and every check is a request, so the router, the
//! owner scoping, the validation, the limits and the storage are exercised together rather
//! than one function at a time.
//!
//! They need a database of their own — one whose tables this binary may drop and
//! rewrite — so they run only when `TEST_DATABASE_URL` names one, and say so and
//! pass otherwise. `cargo test` on a machine without Postgres still passes; CI
//! sets the variable (see `.github/workflows/ci.yml`).
//!
//! Each test makes its own browser id, so tests sharing one database neither see
//! nor disturb each other's rows.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::Method;
use serde_json::{json, Value};

/// How long the server has to answer `/api/health` — migrations, the catalogue
/// and every background task start before it listens.
const READY: Duration = Duration::from_secs(60);

/// The migrations are the same for every test, and two servers starting at once
/// must not race each other's DDL: the first test to ask runs them, the rest
/// find them done.
static MIGRATED: Mutex<bool> = Mutex::new(false);

/// The test database, or None when the caller asked for no integration tests.
fn database() -> Option<String> {
    match std::env::var("TEST_DATABASE_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("skipping server integration tests: set TEST_DATABASE_URL to a Postgres database they may write to");
            None
        }
    }
}

async fn migrated(url: &str) {
    let mut done = MIGRATED.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *done {
        return;
    }
    let mut db = tradoor_store::Db::connect(url).await.unwrap_or_else(|e| panic!("connecting to TEST_DATABASE_URL: {e}"));
    db.migrate().await.expect("running the migrations");
    *done = true;
}

/// A running server, killed and its data directory removed when the test ends.
struct Server {
    child: Child,
    base: String,
    data: PathBuf,
    http: reqwest::Client,
}

impl Server {
    /// Starts the binary, or returns None when no test database was named.
    async fn start() -> Option<Self> {
        let url = database()?;
        migrated(&url).await;

        // A free port, for the moment between closing this listener and the
        // server binding it. A collision fails the test with "binding", which is
        // the honest outcome.
        let port = TcpListener::bind("127.0.0.1:0").expect("a free port").local_addr().expect("local addr").port();
        let data = std::env::temp_dir().join(format!("tradoor-it-{}-{port}", std::process::id()));
        std::fs::create_dir_all(&data).expect("data directory");
        let log = data.join("server.log");
        let out = std::fs::File::create(data.join("server.out")).expect("stdout file");
        let err = std::fs::File::create(&log).expect("stderr file");

        let child = Command::new(env!("CARGO_BIN_EXE_tradoor-server"))
            .arg("serve")
            .env("DATABASE_URL", &url)
            .env("TRADOOR_DATA_DIR", &data)
            .env("TRADOOR_BIND", format!("127.0.0.1:{port}"))
            // The tests want the API, not the exchange collectors behind it.
            .env("TRADOOR_LIQUIDATIONS", "")
            .env("TRADOOR_OPEN_INTEREST", "")
            .env("TRADOOR_FOOTPRINT_HISTORY", "")
            .env("TRADOOR_TELEGRAM_TOKEN", "")
            .env("TRADOOR_SITE_URL", "")
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .expect("starting tradoor-server");

        let server = Self { child, base: format!("http://127.0.0.1:{port}"), data, http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build().expect("http client") };
        server.wait_ready(&log).await;
        Some(server)
    }

    async fn wait_ready(&self, log: &PathBuf) {
        let deadline = Instant::now() + READY;
        while Instant::now() < deadline {
            if let Ok(res) = self.http.get(self.url("/api/health")).send().await {
                if res.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let said = std::fs::read_to_string(log).unwrap_or_default();
        panic!("the server was not ready within {READY:?}; its stderr said:\n{said}");
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// One request as `owner`, with `body` sent as JSON when there is one.
    async fn call(&self, method: Method, path: &str, owner: Option<&str>, body: Option<Value>) -> (reqwest::StatusCode, Value) {
        self.call_from(None, method, path, owner, body).await
    }

    /// The same from a visitor's address: `x-real-ip`, the header nginx sets for
    /// the address it accepted the connection from. Without one — a developer's
    /// machine, or an nginx location that does not set it — the per-address
    /// limits do not apply, which is why every other test here goes without.
    async fn call_from(&self, from: Option<&str>, method: Method, path: &str, owner: Option<&str>, body: Option<Value>) -> (reqwest::StatusCode, Value) {
        let res = self.request(from, method, path, owner, body).await;
        let status = res.status();
        (status, parse(res).await)
    }

    /// The answer itself, for its headers or its status alone.
    async fn request(&self, from: Option<&str>, method: Method, path: &str, owner: Option<&str>, body: Option<Value>) -> reqwest::Response {
        let mut req = self.http.request(method, self.url(path));
        if let Some(from) = from {
            req = req.header("x-real-ip", from);
        }
        if let Some(owner) = owner {
            req = req.header("x-tradoor-owner", owner);
        }
        if let Some(body) = body {
            req = req.header(reqwest::header::CONTENT_TYPE, "application/json").body(serde_json::to_vec(&body).expect("a body"));
        }
        req.send().await.expect("the server to answer")
    }

    /// A browser id no other test in this or an earlier run made.
    fn owner(&self, tag: &str) -> String {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("a clock").as_nanos();
        format!("it-{tag}-{nanos:032x}")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data);
    }
}

/// The JSON an answer carries; `Null` when it carries none, so a check on the
/// status still reads the failure rather than an `unwrap`.
async fn parse(res: reqwest::Response) -> Value {
    let text = res.text().await.unwrap_or_default();
    serde_json::from_str(&text).unwrap_or_else(|_| Value::Null)
}

fn drawings_path(market: &str, timeframe: &str) -> String {
    format!("/api/drawings?market={market}&timeframe={timeframe}")
}

/// `/api/relay/rest` asking for `url`, escaped as a query value.
fn relay_rest(url: &str) -> String {
    let mut path = reqwest::Url::parse("http://relay.invalid/api/relay/rest").expect("a base");
    path.query_pairs_mut().append_pair("url", url);
    format!("{}?{}", path.path(), path.query().unwrap_or_default())
}

/// Addresses from the ranges the documentation reserves for examples: the
/// limiter only string-compares them, and this is what nginx would put there.
const VISITOR: &str = "203.0.113.7";
const ANOTHER_VISITOR: &str = "198.51.100.9";

fn trendline() -> Value {
    json!({
        "kind": "trendline",
        "points": [{ "time": 1_780_000_000_000i64, "price": 62_000 }, { "time": 1_780_003_600_000i64, "price": 63_500 }],
        "style": { "color": "#aafd35", "width": 2 },
        "label": "entry",
    })
}

#[tokio::test]
async fn health_answers_once_the_database_is_migrated() {
    let Some(server) = Server::start().await else { return };
    let res = server.http.get(server.url("/api/health")).send().await.expect("health");
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["x-content-type-options"], "nosniff");
    assert_eq!(res.headers()["x-frame-options"], "DENY");
    assert_eq!(res.headers()["referrer-policy"], "strict-origin-when-cross-origin");
    let body = parse(res).await;
    assert_eq!(body, json!({ "ok": true, "db": true }));
}

#[tokio::test]
async fn a_drawing_is_created_read_back_changed_and_deleted() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("round-trip");
    let path = drawings_path("binance-perp:BTCUSDT", "1h");

    let (status, created) = server.call(Method::POST, &path, Some(&owner), Some(trendline())).await;
    assert_eq!(status, 201, "{created}");
    let drawing = &created["drawing"];
    let id = drawing["id"].as_i64().expect("an id");
    assert_eq!(drawing["kind"], "trendline");
    assert_eq!(drawing["market"], "binance-perp:BTCUSDT");
    assert_eq!(drawing["timeframe"], "1h");
    assert_eq!(drawing["label"], "entry");
    assert_eq!(drawing["style"], json!({ "color": "#aafd35", "width": 2 }));
    assert_eq!(drawing["points"].as_array().expect("points").len(), 2);
    assert!(drawing["createdAt"].as_i64().is_some_and(|t| t > 0), "creation time: {drawing}");

    let (status, listed) = server.call(Method::GET, &path, Some(&owner), None).await;
    assert_eq!(status, 200);
    assert_eq!(listed["count"], 1);
    assert_eq!(listed["drawings"][0]["id"], id);

    let moved = json!({ "points": [{ "time": 1_780_000_000_000i64, "price": 61_000 }, { "time": 1_780_003_600_000i64, "price": 64_000 }], "label": "moved" });
    let (status, changed) = server.call(Method::PATCH, &format!("/api/drawings/{id}"), Some(&owner), Some(moved)).await;
    assert_eq!(status, 200, "{changed}");
    assert_eq!(changed["drawing"]["label"], "moved");
    assert_eq!(changed["drawing"]["points"][1]["price"].as_f64(), Some(64_000.0));
    assert_eq!(changed["drawing"]["kind"], "trendline", "a change never moves a drawing to another kind");

    let (status, refused) = server.call(Method::PATCH, &format!("/api/drawings/{id}"), Some(&owner), Some(json!({}))).await;
    assert_eq!(status, 400);
    assert!(refused["error"].as_str().is_some_and(|e| e.contains("nothing to change")), "{refused}");

    let (status, deleted) = server.call(Method::DELETE, &format!("/api/drawings/{id}"), Some(&owner), None).await;
    assert_eq!(status, 200);
    assert_eq!(deleted, json!({ "id": id }));

    let (status, listed) = server.call(Method::GET, &path, Some(&owner), None).await;
    assert_eq!((status, listed["count"].as_i64()), (reqwest::StatusCode::OK, Some(0)));

    let (status, missing) = server.call(Method::DELETE, &format!("/api/drawings/{id}"), Some(&owner), None).await;
    assert_eq!(status, 404, "{missing}");
}

#[tokio::test]
async fn a_browser_sees_and_changes_only_its_own_drawings() {
    let Some(server) = Server::start().await else { return };
    let (mine, yours) = (server.owner("mine"), server.owner("yours"));
    let path = drawings_path("binance-perp:BTCUSDT", "4h");

    let (status, created) = server.call(Method::POST, &path, Some(&mine), Some(trendline())).await;
    assert_eq!(status, 201);
    let id = created["drawing"]["id"].as_i64().expect("an id");

    let (status, listed) = server.call(Method::GET, &path, Some(&yours), None).await;
    assert_eq!(status, 200);
    assert_eq!(listed["count"], 0, "another browser's chart is empty");

    // Someone else's drawing is not found, not forbidden: its existence is not
    // theirs to learn.
    let (status, _) = server.call(Method::DELETE, &format!("/api/drawings/{id}"), Some(&yours), None).await;
    assert_eq!(status, 404);
    let (status, _) = server.call(Method::PATCH, &format!("/api/drawings/{id}"), Some(&yours), Some(json!({ "label": "stolen" }))).await;
    assert_eq!(status, 404);

    let (status, listed) = server.call(Method::GET, &path, Some(&mine), None).await;
    assert_eq!((status, listed["count"].as_i64(), listed["drawings"][0]["label"].as_str()), (reqwest::StatusCode::OK, Some(1), Some("entry")));
}

#[tokio::test]
async fn a_request_must_name_a_well_formed_browser_and_chart() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("bad-request");
    let path = drawings_path("binance-perp:BTCUSDT", "1h");

    let (status, body) = server.call(Method::GET, &path, None, None).await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().is_some_and(|e| e.contains("x-tradoor-owner")), "{body}");

    let (status, _) = server.call(Method::GET, &path, Some("short"), None).await;
    assert_eq!(status, 400, "a browser id is at least 16 characters");

    let (status, body) = server.call(Method::GET, &drawings_path("BTCUSDT", "1h"), Some(&owner), None).await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().is_some_and(|e| e.contains("not a market")), "{body}");

    let (status, body) = server.call(Method::GET, &drawings_path("binance-perp:BTCUSDT", "7h"), Some(&owner), None).await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().is_some_and(|e| e.contains("invalid timeframe")), "{body}");

    let (status, body) = server.call(Method::GET, &format!("{path}&chart=volume"), Some(&owner), None).await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().is_some_and(|e| e.contains("chart is candles")), "{body}");
}

#[tokio::test]
async fn a_drawing_that_cannot_be_drawn_is_not_stored() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("validation");
    let path = drawings_path("binance-perp:BTCUSDT", "1h");
    let point = json!({ "time": 1, "price": 2 });

    let rejected = [
        (json!({ "kind": "gann", "points": [point] }), "invalid kind"),
        (json!({ "points": [point] }), "invalid kind"),
        (json!({ "kind": "text", "points": [point] }), "needs a label"),
        (json!({ "kind": "trendline", "points": [point] }), "needs exactly 2 points"),
        (json!({ "kind": "horizontal", "points": [point, point] }), "needs exactly 1 point"),
        (json!({ "kind": "horizontal", "points": "nope" }), "points must be an array"),
        (json!({ "kind": "horizontal", "points": [{ "time": 1 }] }), "price must be a finite"),
        (json!({ "kind": "horizontal", "points": [point], "style": { "levels": [1, 2] } }), "style.levels must be"),
        (json!({ "kind": "brush", "points": vec![point; 2001] }), "at most 2000 points"),
    ];
    for (body, needle) in rejected {
        let (status, answer) = server.call(Method::POST, &path, Some(&owner), Some(body.clone())).await;
        assert_eq!(status, 400, "{body} was accepted");
        assert!(answer["error"].as_str().is_some_and(|e| e.contains(needle)), "{body}: {answer}");
    }

    let (status, listed) = server.call(Method::GET, &path, Some(&owner), None).await;
    assert_eq!((status, listed["count"].as_i64()), (reqwest::StatusCode::OK, Some(0)), "a rejected drawing leaves no row behind");
}

#[tokio::test]
async fn drawings_belong_to_one_market_on_one_chart() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("scope");
    let candles = drawings_path("binance-perp:BTCUSDT", "1h");
    let tpo = format!("{candles}&chart=tpo");
    let other = drawings_path("bybit-perp:ETHUSDT", "1h");

    assert_eq!(server.call(Method::POST, &candles, Some(&owner), Some(trendline())).await.0, 201);
    assert_eq!(server.call(Method::POST, &tpo, Some(&owner), Some(trendline())).await.0, 201);

    let (_, listed) = server.call(Method::GET, &candles, Some(&owner), None).await;
    assert_eq!(listed["count"], 1, "the TPO chart's markings are not the candles chart's");
    let (_, listed) = server.call(Method::GET, &other, Some(&owner), None).await;
    assert_eq!(listed["count"], 0, "another market has its own chart");

    let (status, cleared) = server.call(Method::DELETE, &tpo, Some(&owner), None).await;
    assert_eq!((status, cleared["deleted"].as_i64()), (reqwest::StatusCode::OK, Some(1)));
    let (_, listed) = server.call(Method::GET, &candles, Some(&owner), None).await;
    assert_eq!(listed["count"], 1, "clearing one chart leaves the other alone");
    let (_, listed) = server.call(Method::GET, &tpo, Some(&owner), None).await;
    assert_eq!(listed["count"], 0);
}

#[tokio::test]
async fn a_workspace_write_carries_the_version_it_was_based_on() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("workspace");
    let live = "/api/workspaces/default";

    let (status, created) = server.call(Method::PUT, live, Some(&owner), Some(json!({ "name": "Terminal", "doc": {}, "version": 0 }))).await;
    assert_eq!(status, 201, "{created}");
    assert_eq!(created["version"], 1);

    let (status, read) = server.call(Method::GET, live, Some(&owner), None).await;
    assert_eq!(status, 200);
    assert_eq!((read["name"].as_str(), read["doc"].clone(), read["version"].as_i64()), (Some("Terminal"), json!({}), Some(1)));

    let (status, saved) = server.call(Method::PUT, live, Some(&owner), Some(json!({ "name": "Terminal", "doc": { "panels": 2 }, "version": 1 }))).await;
    assert_eq!((status, saved["version"].as_i64()), (reqwest::StatusCode::OK, Some(2)));

    // A second tab still holding version 1 must not overwrite the second write.
    let (status, stale) = server.call(Method::PUT, live, Some(&owner), Some(json!({ "name": "Terminal", "doc": { "panels": 3 }, "version": 1 }))).await;
    assert_eq!(status, 409, "{stale}");
    assert_eq!(stale["doc"], json!({ "panels": 2 }), "the conflict hands back what is there now");
    assert_eq!(stale["version"], 2);

    let (status, _) = server.call(Method::PUT, live, Some(&owner), Some(json!({ "name": "Terminal", "doc": [1], "version": 2 }))).await;
    assert_eq!(status, 400, "a document is an object");

    // Named layouts are for signed-in readers; the one in use is not.
    let (status, refused) = server.call(Method::PUT, "/api/workspaces/scalping", Some(&owner), Some(json!({ "name": "Scalping", "doc": {}, "version": 0 }))).await;
    assert_eq!(status, 403, "{refused}");
    assert!(refused["error"].as_str().is_some_and(|e| e.contains("sign in")), "{refused}");

    let (status, listed) = server.call(Method::GET, "/api/workspaces", Some(&owner), None).await;
    assert_eq!(status, 200);
    assert_eq!(listed["layouts"], json!([]), "the workspace in use is not a saved layout");

    let (status, _) = server.call(Method::GET, "/api/workspaces/scalping", Some(&owner), None).await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn a_visitor_past_its_request_burst_is_told_to_wait() {
    let Some(server) = Server::start().await else { return };

    // The widest bucket is the address's own: everything it asks counts, whatever
    // the kind. Buckets refill as the loop runs, so what a test can fix is that
    // the burst is served and that a refusal follows it.
    let mut served = 0;
    let mut refused = None;
    for _ in 0..400 {
        let res = server.request(Some(VISITOR), Method::GET, "/api/health", None, None).await;
        if res.status() == 429 {
            refused = Some(res);
            break;
        }
        assert_eq!(res.status(), 200, "a request inside the burst");
        served += 1;
    }
    assert!(served >= 300, "the burst is 300 requests; {served} were served before the refusal");
    let res = refused.expect("the request burst to run out");
    assert_eq!(res.headers()["retry-after"], "10", "the refusal says when to come back");
    let answer = parse(res).await;
    assert!(answer["error"].as_str().is_some_and(|e| e.contains("too many requests")), "{answer}");

    // Another address has a bucket of its own, and a request carrying no address
    // at all — the developer's own server — is not capped here at all.
    assert_eq!(server.call_from(Some(ANOTHER_VISITOR), Method::GET, "/api/health", None, None).await.0, 200);
    assert_eq!(server.call(Method::GET, "/api/health", None, None).await.0, 200);
}

#[tokio::test]
async fn a_visitor_past_its_write_burst_is_told_to_wait() {
    let Some(server) = Server::start().await else { return };
    let owner = server.owner("write-burst");
    let path = drawings_path("binance-perp:BTCUSDT", "1h");

    // Writes have their own, tighter bucket: a page redraws far more often than
    // it saves, and the saves are what grow the tables.
    let mut wrote = 0;
    let mut refused = None;
    for _ in 0..80 {
        let res = server.request(Some(VISITOR), Method::POST, &path, Some(&owner), Some(trendline())).await;
        if res.status() == 429 {
            refused = Some(res);
            break;
        }
        assert_eq!(res.status(), 201, "a write inside the burst");
        wrote += 1;
    }
    assert!(wrote >= 60, "the burst is 60 writes; {wrote} were kept before the refusal");
    let res = refused.expect("the write burst to run out");
    assert_eq!(res.headers()["retry-after"], "10");
    assert_eq!(res.headers()["x-frame-options"], "DENY", "a refusal still leaves through the security headers");
    let answer = parse(res).await;
    assert!(answer["error"].as_str().is_some_and(|e| e.contains("too many writes")), "{answer}");

    // A refused write is not a write: the chart holds exactly what got through.
    let (status, listed) = server.call(Method::GET, &path, Some(&owner), None).await;
    assert_eq!((status, listed["count"].as_i64()), (reqwest::StatusCode::OK, Some(wrote)));
}

#[tokio::test]
async fn a_flood_of_fresh_browser_ids_is_cut_off_per_address() {
    let Some(server) = Server::start().await else { return };

    // A browser id costs nothing to invent, so what is rationed is the first rows
    // a new one would make: twenty ids an address, then no more from it.
    let path = drawings_path("binance-perp:BTCUSDT", "1h");
    let mut made = 0;
    let mut settled = None;
    let mut refused = None;
    for _ in 0..25 {
        let owner = server.owner("fresh-ids");
        let (status, answer) = server.call_from(Some(VISITOR), Method::POST, &path, Some(&owner), Some(trendline())).await;
        if status == 429 {
            refused = Some(answer);
            break;
        }
        assert_eq!(status, 201, "{answer}");
        if settled.is_none() {
            settled = Some(owner);
        }
        made += 1;
    }
    assert!(made >= 20, "the burst is twenty new ids; {made} were accepted before the refusal");
    let answer = refused.expect("the new-id burst to run out");
    assert!(answer["error"].as_str().is_some_and(|e| e.contains("too many new browser ids")), "{answer}");

    // The cap is on ids, not on the address: one it already has keeps working.
    let settled = settled.expect("one new id to have been accepted");
    let (status, answer) = server.call_from(Some(VISITOR), Method::POST, &path, Some(&settled), Some(trendline())).await;
    assert_eq!(status, 201, "a browser id the address already has is not a new one: {answer}");
}

#[tokio::test]
async fn the_relay_carries_only_market_data_and_rations_an_address() {
    let Some(server) = Server::start().await else { return };

    // The allow-list is checked before anything leaves the server, so these
    // refusals cost no connection to an exchange.
    for url in [
        "https://evil.example/fapi/v1/klines",
        "https://fapi.binance.com/fapi/v1/order",
        "http://fapi.binance.com/fapi/v1/klines",
        "https://fapi.binance.com:8443/fapi/v1/klines",
        "https://fapi.binance.com/fapi/v1/klines/../order",
    ] {
        let (status, answer) = server.call(Method::GET, &relay_rest(url), None, None).await;
        assert_eq!(status, 403, "{url} was not refused: {answer}");
        assert!(answer["error"].as_str().is_some_and(|e| e.contains("market data")), "{url}: {answer}");
    }
    let (status, answer) = server.call(Method::GET, &relay_rest("nonsense"), None, None).await;
    assert_eq!((status, answer["error"].as_str()), (reqwest::StatusCode::BAD_REQUEST, Some("not a URL")));

    // A stream for a venue no exchange socket answers to. The upgrade is offered
    // so the handler is reached rather than refused for the missing handshake;
    // the answer is a plain refusal, as no 101 is ever sent for it.
    let res = server.http.get(server.url("/api/relay/ws?venue=wss%3A%2F%2Fanything")).header("connection", "upgrade").header("upgrade", "websocket").header("sec-websocket-version", "13").header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==").send().await.expect("a stream request");
    let status = res.status();
    let answer = parse(res).await;
    assert_eq!(status, 400, "a venue with no stream is refused: {answer}");
    assert!(answer["error"].as_str().is_some_and(|e| e.contains("unknown venue")), "{answer}");

    // Relayed calls are rationed per address too. A refused call makes no upstream
    // request, so this loop stays just as cheap as the ones above. What is not
    // reached here is the per-exchange cap: it is taken only once an allowed URL
    // passes, which would mean calling the exchange.
    let denied = relay_rest("https://evil.example/fapi/v1/klines");
    let mut asked = 0;
    let mut refused = None;
    for _ in 0..130 {
        let res = server.request(Some(VISITOR), Method::GET, &denied, None, None).await;
        if res.status() == 429 {
            refused = Some(res);
            break;
        }
        assert_eq!(res.status(), 403);
        asked += 1;
    }
    assert!(asked >= 120, "the burst is 120 relayed calls; {asked} were answered before the refusal");
    let res = refused.expect("the relay burst to run out");
    assert_eq!(res.headers()["retry-after"], "10");
    let answer = parse(res).await;
    assert!(answer["error"].as_str().is_some_and(|e| e.contains("too many relay-rest")), "{answer}");
}
