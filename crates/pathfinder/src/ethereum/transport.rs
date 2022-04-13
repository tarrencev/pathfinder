use std::iter::Peekable;
use std::time::Duration;

use web3::Web3;

use crate::config::EthereumConfig;
use crate::ethereum::Chain;

/// An [Ethereum HTTP transport](Web3<web3::transports::Http>) wrapper which adds auto-retry with
/// exponential backoff behavior.
pub struct Transport {
    inner: Web3<web3::transports::Http>,
}

impl Transport {
    /// Creates an Ethereum [Transport] based on the [config](EthereumConfig).
    pub fn new(config: EthereumConfig) -> anyhow::Result<Self> {
        use anyhow::Context;

        let client = reqwest::Client::builder();
        let client = match config.user_agent {
            Some(user_agent) => client.user_agent(user_agent),
            None => client,
        }
        .build()
        .context("Creating HTTP client")?;

        let mut url = config.url;
        url.set_password(config.password.as_deref())
            .map_err(|_| anyhow::anyhow!("Setting password"))?;

        let client = web3::transports::Http::with_client(client, url);
        let inner = Web3::new(client);

        Ok(Self { inner })
    }

    #[cfg(test)]
    /// Creates a [Transport] for the given Ethereum [Chain] using the relevant environment
    /// variables asdasd uik qwke  koia shdkjah for the .
    ///
    /// Requires an environment variable for both the URL and (optional) password.
    ///
    /// Panics if the environment variables are not specified.
    ///
    /// Goerli:  PATHFINDER_ETHEREUM_HTTP_GOERLI_URL
    ///          PATHFINDER_ETHEREUM_HTTP_GOERLI_PASSWORD (optional)
    ///
    /// Mainnet: PATHFINDER_ETHEREUM_HTTP_MAINNET_URL
    ///          PATHFINDER_ETHEREUM_HTTP_MAINNET_PASSWORD (optional)
    pub fn test(chain: Chain) -> Self {
        let key_prefix = match chain {
            Chain::Mainnet => "PATHFINDER_ETHEREUM_HTTP_MAINNET",
            Chain::Goerli => "PATHFINDER_ETHEREUM_HTTP_GOERLI",
        };

        let url_key = format!("{}_URL", key_prefix);
        let password_key = format!("{}_PASSWORD", key_prefix);

        let url = std::env::var(&url_key)
            .unwrap_or_else(|_| panic!("Ethereum URL environment var not set {url_key}"));
        let password = std::env::var(password_key).ok();
        let url = url.parse::<reqwest::Url>().expect("Bad Ethereum URL");

        let config = EthereumConfig {
            url,
            password,
            user_agent: None,
        };

        Self::new(config).unwrap()
    }

    pub async fn retry_chain(&self) -> anyhow::Result<Chain> {
        use web3::error::TransportError;
        use web3::types::U256;
        use web3::Error::*;

        let backoff_strategy = BackoffStrategy::new(vec![
            Duration::from_secs(2),
            Duration::from_secs(10),
            Duration::from_secs(30),
            Duration::from_secs(120),
            Duration::from_secs(300),
        ]);

        let mut backoff_iter = backoff_strategy.iter();

        const TOO_MANY_REQUESTS: u16 = 429;
        const BAD_GATEWAY: u16 = 502;
        const SERVICE_UNAVAILABLE: u16 = 503;
        const GATEWAY_TIMEOUT: u16 = 504;

        loop {
            let result = self.inner.eth().chain_id().await;

            match result {
                Ok(chain_id) => match chain_id {
                    id if id == U256::from(1u32) => return Ok(Chain::Mainnet),
                    id if id == U256::from(5u32) => return Ok(Chain::Goerli),
                    other => anyhow::bail!("Unsupported chain ID: {}", other),
                },
                Err(e) => match e {
                    Unreachable
                    | Transport(TransportError::Code(TOO_MANY_REQUESTS))
                    | Transport(TransportError::Code(BAD_GATEWAY))
                    | Transport(TransportError::Code(SERVICE_UNAVAILABLE))
                    | Transport(TransportError::Code(GATEWAY_TIMEOUT)) => {
                        let backoff = backoff_iter.next();
                        tracing::debug!(reason=%e, backoff=?backoff, "Transient error, retrying after backing off");
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    other => anyhow::bail!(other),
                },
            }
        }
    }
}

/// Describes a backoff strategy as a sequence of [Durations](Duration)
/// to wait between retries. Access to these is provided using [BackoffStrategy::iter].
pub struct BackoffStrategy {
    values: Vec<Duration>,
}

impl BackoffStrategy {
    pub fn new(values: Vec<Duration>) -> Self {
        Self { values }
    }

    pub fn iter(&self) -> BackoffIter {
        BackoffIter {
            inner: IterRepeat::Iter(self.values.iter().peekable()),
        }
    }
}

/// An iterator over [Duration] which repeats the final element once the initial iterator is
/// exhausted.
pub struct BackoffIter<'a> {
    inner: IterRepeat<'a>,
}

enum IterRepeat<'a> {
    Iter(Peekable<core::slice::Iter<'a, Duration>>),
    Repeat(Duration),
}   

impl<'a> BackoffIter<'a> {
    pub fn next(&mut self) -> Duration {
        match &mut self.inner {
            IterRepeat::Iter(inner) => match inner.next() {
                Some(item) => {
                    if inner.peek().is_none() {
                        self.inner = IterRepeat::Repeat(*item);
                    }

                    *item
                }
                None => Duration::ZERO,
            },
            IterRepeat::Repeat(inner) => *inner,
        }
    }
}
