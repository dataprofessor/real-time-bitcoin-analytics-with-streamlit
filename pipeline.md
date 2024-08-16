```mermaid
%%{ init: { 'flowchart': { 'curve': 'monotoneX' } } }%%
graph LR;
bitcoin_source_producer[fa:fa-rocket bitcoin_source_producer &#8205] --> trades-raw{{ fa:fa-arrow-right-arrow-left trades-raw &#8205}}:::topic;
trades-raw{{ fa:fa-arrow-right-arrow-left trades-raw &#8205}}:::topic --> ohlc_aggregator[fa:fa-rocket ohlc_aggregator &#8205];
ohlc_aggregator[fa:fa-rocket ohlc_aggregator &#8205] --> trades-ohlc{{ fa:fa-arrow-right-arrow-left trades-ohlc &#8205}}:::topic;
trades-ohlc{{ fa:fa-arrow-right-arrow-left trades-ohlc &#8205}}:::topic --> database_sink[fa:fa-rocket database_sink &#8205];


classDef default font-size:110%;
classDef topic font-size:80%;
classDef topic fill:#3E89B3;
classDef topic stroke:#3E89B3;
classDef topic color:white;
```