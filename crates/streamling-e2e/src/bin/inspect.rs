//! Binary for inspecting e2e test resources.

use std::env;
use streamling_e2e::inspect;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <test_uuid>", args[0]);
        eprintln!("Example: {} test_d9b2cb44", args[0]);
        std::process::exit(1);
    }

    let test_uuid = &args[1];
    match inspect::inspect_test(test_uuid).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
