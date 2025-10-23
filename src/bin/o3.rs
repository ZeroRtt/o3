use std::{
    io::{Error, Result},
    time::Duration,
};

use clap::Parser;
use color_print::ceprintln;
use metricrs::global::set_global_registry;
use metricrs_protobuf::registry::ProtoBufRegistry;
use o3::{
    cli::{Cli, Commands},
    metrics::MetricsPrint,
    redirect::Redirect,
};
use zrquic::{
    poll::server::{Acceptor, SimpleAddressValidator},
    quiche,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    if let Err(err) = run(cli).await {
        ceprintln!("<s><r>error:</r></s> {}", err)
    }
}

async fn run(cli: Cli) -> Result<()> {
    if cli.debug {
        pretty_env_logger::try_init_timed().map_err(Error::other)?;
    }

    if let Some(laddr) = cli.metrics {
        let registry = ProtoBufRegistry::bind(laddr)?;
        let laddr = registry.local_addr();
        set_global_registry(registry).unwrap();

        let mut fetch = MetricsPrint::connect(laddr)?;

        std::thread::spawn(move || {
            if let Err(err) = fetch.run() {
                log::error!("background metrics worker is stopped, {}", err);
            }
        });
    }

    #[allow(irrefutable_let_patterns)]
    if let Commands::Redirect { target } = cli.commands {
        let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

        cli.quiche_config(&mut config)?;

        let rproxy = Redirect::new(
            cli.parse_redirect_listen_addrs()?,
            target,
            Acceptor::new(config, SimpleAddressValidator::new(Duration::from_secs(60))),
        )?;

        rproxy.run().await?;
    }

    Ok(())
}
