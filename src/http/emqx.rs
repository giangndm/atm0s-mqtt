use poem::Route;
use poem_openapi::OpenApiService;
use tokio::sync::mpsc::Sender;

use super::HttpCommand;

mod v5;

pub fn route(cmd_tx: Sender<HttpCommand>) -> poem::Route {
    let v5_api: OpenApiService<_, ()> = OpenApiService::new(v5::EmqxV5::new(cmd_tx), "Emqx V5", env!("CARGO_PKG_VERSION")).server("/api/v5");
    let docs_ui = v5_api.swagger_ui();

    Route::new().nest("/v5", v5_api).nest("/docs", docs_ui)
}
