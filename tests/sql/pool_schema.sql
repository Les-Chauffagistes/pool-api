-- Schéma MINIMAL des tables lues par les DAO (Users, PoolStat, MonthlyBests, History).
-- ATTENTION : déduit des requêtes SQL du dépôt, pas copié de la source de vérité (ckpool-share-exporter).
-- À remplacer par une copie de ses migrations dès qu'elles sont accessibles, et à garder alignée avec elles.

CREATE TABLE pool_stat
(
    pool_instance text PRIMARY KEY,
    updated_at    timestamptz NOT NULL,
    runtime_s     bigint      NOT NULL,
    users         bigint      NOT NULL,
    workers       bigint      NOT NULL,
    idle          bigint      NOT NULL,
    disconnected  bigint      NOT NULL,
    hashrate_1m   bigint      NOT NULL,
    hashrate_5m   bigint      NOT NULL,
    hashrate_15m  bigint      NOT NULL,
    hashrate_1h   bigint      NOT NULL,
    hashrate_6h   bigint      NOT NULL,
    hashrate_1d   bigint      NOT NULL,
    hashrate_7d   bigint      NOT NULL,
    diff          double precision NOT NULL,
    accepted      bigint      NOT NULL,
    rejected      bigint      NOT NULL,
    bestshare     bigint      NOT NULL,
    sps_1m        double precision NOT NULL,
    sps_5m        double precision NOT NULL,
    sps_15m       double precision NOT NULL,
    sps_1h        double precision NOT NULL
);

CREATE TABLE users
(
    pool_instance text        NOT NULL,
    address       text        NOT NULL,
    hashrate1m    bigint      NOT NULL DEFAULT 0,
    hashrate5m    bigint      NOT NULL DEFAULT 0,
    hashrate1hr   bigint      NOT NULL DEFAULT 0,
    hashrate1d    bigint      NOT NULL DEFAULT 0,
    hashrate7d    bigint      NOT NULL DEFAULT 0,
    lastshare     timestamptz NOT NULL,
    workers       bigint      NOT NULL DEFAULT 0,
    shares        bigint      NOT NULL DEFAULT 0,
    bestshare     bigint      NOT NULL DEFAULT 0,
    authorized    timestamptz NOT NULL,
    PRIMARY KEY (pool_instance, address)
);

CREATE TABLE user_workers
(
    pool_instance text        NOT NULL,
    address       text        NOT NULL,
    workername    text        NOT NULL,
    hashrate1m    bigint      NOT NULL DEFAULT 0,
    hashrate5m    bigint      NOT NULL DEFAULT 0,
    hashrate1hr   bigint      NOT NULL DEFAULT 0,
    hashrate1d    bigint      NOT NULL DEFAULT 0,
    hashrate7d    bigint      NOT NULL DEFAULT 0,
    lastshare     timestamptz NOT NULL,
    shares        bigint      NOT NULL DEFAULT 0,
    bestshare     bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (pool_instance, address, workername)
);

CREATE TABLE monthly_bests
(
    month      date             NOT NULL,
    best_diff  double precision NOT NULL,
    "user"     text             NOT NULL,
    workername text             NOT NULL,
    updated_at timestamptz      NOT NULL,
    PRIMARY KEY (month, "user", workername)
);

CREATE TABLE share_weights_hourly
(
    workername text             NOT NULL,
    hour       timestamptz      NOT NULL,
    shares     bigint           NOT NULL,
    diff_sum   double precision NOT NULL,
    PRIMARY KEY (workername, hour)
);

CREATE TABLE worker_stats_1d
(
    day           date             NOT NULL,
    workername    text             NOT NULL,
    avg_hashrate1h double precision NOT NULL,
    PRIMARY KEY (day, workername)
);
