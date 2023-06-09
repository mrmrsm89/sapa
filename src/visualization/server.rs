use super::dump::{dump_ledger, dump_picker_timestamp};
use crate::blockchain::BlockChain;
use crate::blockdb::BlockDatabase;
use crate::utxodb::UtxoDatabase;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tiny_http::Header;
use tiny_http::Response;
use tiny_http::Server as HTTPServer;
use url::Url;

pub struct Server {
    blockchain: Arc<BlockChain>,
    blockdb: Arc<BlockDatabase>,
    utxodb: Arc<UtxoDatabase>,
    handle: HTTPServer,
}

/// This macro serves the static file at the location `path` and attaches the content type `type`.
macro_rules! serve_static_file {
    ( $req:expr, $path:expr, $type:expr ) => {{
        let content_type = concat!("Content-Type: ", $type).parse::<Header>().unwrap();
        let cache_control = "Cache-Control: public, max-age=31536000"
            .parse::<Header>()
            .unwrap();
        let resp = Response::from_string(include_str!($path))
            .with_header(content_type)
            .with_header(cache_control);
        $req.respond(resp).unwrap();
    }};
}

/// This macro serves the string `src` and attaches the content type `type`. Before serving the
/// string, all occurrances of `SERVER_IP_ADDR` and `SERVER_PORT_NUMBER` in the string are replaced
/// with the server IP and port respectively.
macro_rules! serve_dynamic_file {
    ( $req:expr, $src:expr, $type:expr, $addr:expr ) => {{
        let source = $src
            .to_string()
            .replace("SERVER_IP_ADDR", &$addr.ip().to_string())
            .replace("SERVER_PORT_NUMBER", &$addr.port().to_string());
        let content_type = concat!("Content-Type: ", $type).parse::<Header>().unwrap();
        let cache_control = "Cache-Control: no-store".parse::<Header>().unwrap();
        let allow_all = "Access-Control-Allow-Origin: *".parse::<Header>().unwrap();
        let resp = Response::from_string(source)
            .with_header(content_type)
            .with_header(cache_control)
            .with_header(allow_all);
        $req.respond(resp).unwrap();
    }};
}

impl Server {
    pub fn start(
        addr: std::net::SocketAddr,
        blockchain: &Arc<BlockChain>,
        blockdb: &Arc<BlockDatabase>,
        utxodb: &Arc<UtxoDatabase>,
    ) {
        let handle = HTTPServer::http(&addr).unwrap();
        let server = Self {
            blockchain: Arc::clone(blockchain),
            blockdb: Arc::clone(blockdb),
            utxodb: Arc::clone(utxodb),
            handle,
        };
        thread::spawn(move || {
            for req in server.handle.incoming_requests() {
                let blockchain = Arc::clone(&server.blockchain);
                let blockdb = Arc::clone(&server.blockdb);
                let utxodb = Arc::clone(&server.utxodb);
                thread::spawn(move || {
                    // a valid url requires a base
                    let base_url = Url::parse(&format!("http://{}/", &addr)).unwrap();
                    let url = match base_url.join(req.url()) {
                        Ok(u) => u,
                        Err(_) => {
                            let content_type = "Content-Type: text/html".parse::<Header>().unwrap();
                            let resp = Response::from_string(include_str!("404.html"))
                                .with_header(content_type)
                                .with_status_code(404);
                            req.respond(resp).expect("respond error");
                            return;
                        }
                    };
                    let params = url.query_pairs();
                    let params: HashMap<_, _> = params.into_owned().collect();
                    let display_fork: bool = params
                        .get("fork")
                        .map_or(false, |s| s.parse::<bool>().unwrap_or(false));
                    let limit: u64 = {
                        const DEFAULT: u64 = 100;
                        params
                            .get("limit")
                            .map_or(DEFAULT, |s| s.parse::<u64>().unwrap_or(DEFAULT))
                    };
                    match url.path() {
                        "/blockchain.json" => serve_dynamic_file!(
                            req,
                            match blockchain.dump(limit, display_fork) {
                                Ok(dump) => dump,
                                Err(_) => "Blockchain Dump error".to_string(),
                            },
                            "application/json",
                            addr
                        ),
                        "/ledger.json" => serve_dynamic_file!(
                            req,
                            dump_ledger(&blockchain, &blockdb, &utxodb, limit),
                            "application/json",
                            addr
                        ),
                        "/picker.json" => serve_dynamic_file!(
                            req,
                            dump_picker_timestamp(&blockchain, &blockdb),
                            "application/json",
                            addr
                        ),
                        "/cytoscape.min.js" => {
                            serve_static_file!(req, "cytoscape.js", "application/javascript")
                        }
                        "/dagre.min.js" => {
                            serve_static_file!(req, "dagre.min.js", "application/javascript")
                        }
                        "/cytoscape-dagre.js" => {
                            serve_static_file!(req, "cytoscape-dagre.js", "application/javascript")
                        }
                        "/bootstrap.min.css" => {
                            serve_static_file!(req, "bootstrap.min.css", "text/css")
                        }
                        "/blockchain_vis.js" => serve_dynamic_file!(
                            req,
                            include_str!("blockchain_vis.js"),
                            "application/javascript",
                            addr
                        ),
                        "/visualize-blockchain" => serve_dynamic_file!(
                            req,
                            include_str!("blockchain_vis.html"),
                            "text/html",
                            addr
                        ),
                        //                    "/ledger_vis.js" => serve_dynamic_file!(
                        //                        req,
                        //                        include_str!("ledger_vis.js"),
                        //                        "application/javascript",
                        //                        addr
                        //                    ),
                        //                    "/visualize-ledger" => {
                        //                        serve_dynamic_file!(req, include_str!("ledger_vis.html"), "text/html", addr)
                        //                    }
                        "/" => {
                            serve_dynamic_file!(req, include_str!("index.html"), "text/html", addr)
                        }
                        _ => {
                            let content_type = "Content-Type: text/html".parse::<Header>().unwrap();
                            let resp = Response::from_string(include_str!("404.html"))
                                .with_header(content_type)
                                .with_status_code(404);
                            req.respond(resp).expect("respond error");
                        }
                    }
                });
            }
        });
    }
}
