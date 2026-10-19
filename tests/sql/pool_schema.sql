-- Schéma des tables lues par les DAO (Users, PoolStat, MonthlyBests, History).
--
-- pool_stat, users, user_workers et monthly_bests : copiés du DDL de production (sans owner ni index inutiles aux tests).
-- A garder alignés avec la production.
--
-- ATTENTION, ces deux objets ne figurent PAS dans le DDL fourni ; leurs colonnes sont DEDUITES des requêtes SQL :
--   - share_weights_hourly (lue par UsersDAO.get_data) : probablement un agrégat continu sur share_weights
--   - worker_stats_1d      (lue par HistoryDAO)        : probablement dans la base d'historique (history_database_url)
-- A remplacer par les vraies définitions dès qu'on les a.

CREATE TABLE pool_stat
(
    pool_instance text                     NOT NULL
        CONSTRAINT pool_stat_pk PRIMARY KEY,
    updated_at    timestamp with time zone NOT NULL,
    runtime_s     bigint                   NOT NULL,
    users         integer                  NOT NULL,
    workers       integer                  NOT NULL,
    idle          integer                  NOT NULL,
    disconnected  integer                  NOT NULL,
    hashrate_1m   double precision         NOT NULL,
    hashrate_5m   double precision         NOT NULL,
    hashrate_15m  double precision         NOT NULL,
    hashrate_1h   double precision         NOT NULL,
    hashrate_6h   double precision         NOT NULL,
    hashrate_1d   double precision         NOT NULL,
    hashrate_7d   double precision         NOT NULL,
    diff          double precision         NOT NULL,
    accepted      bigint                   NOT NULL,
    rejected      bigint                   NOT NULL,
    bestshare     double precision         NOT NULL,
    sps_1m        double precision         NOT NULL,
    sps_5m        double precision         NOT NULL,
    sps_15m       double precision         NOT NULL,
    sps_1h        double precision         NOT NULL
);

CREATE TABLE monthly_bests
(
    month      date                                   NOT NULL
        CONSTRAINT monthly_bests_month_first_day
            CHECK (month = (date_trunc('month'::text, (month)::timestamp with time zone))::date),
    "user"     text                                   NOT NULL,
    best_diff  double precision                       NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    workername text                                   NOT NULL,
    CONSTRAINT monthly_bests_pk PRIMARY KEY (month, "user")
);

CREATE TABLE users
(
    address       text                     NOT NULL,
    hashrate1m    bigint  DEFAULT 0        NOT NULL,
    hashrate5m    bigint  DEFAULT 0        NOT NULL,
    hashrate1hr   bigint  DEFAULT 0        NOT NULL,
    hashrate1d    bigint  DEFAULT 0        NOT NULL,
    hashrate7d    bigint  DEFAULT 0        NOT NULL,
    lastshare     timestamp with time zone NOT NULL,
    workers       integer DEFAULT 0        NOT NULL,
    shares        bigint  DEFAULT 0        NOT NULL,
    bestshare     bigint  DEFAULT 0        NOT NULL,
    authorized    timestamp with time zone NOT NULL,
    pool_instance text                     NOT NULL,
    CONSTRAINT users_pk PRIMARY KEY (address, pool_instance)
);

CREATE TABLE user_workers
(
    workername    text                     NOT NULL,
    pool_instance text                     NOT NULL,
    address       text                     NOT NULL,
    hashrate1m    bigint DEFAULT 0         NOT NULL,
    hashrate5m    bigint DEFAULT 0         NOT NULL,
    hashrate1hr   bigint DEFAULT 0         NOT NULL,
    hashrate1d    bigint DEFAULT 0         NOT NULL,
    hashrate7d    bigint DEFAULT 0         NOT NULL,
    lastshare     timestamp with time zone NOT NULL,
    shares        bigint DEFAULT 0         NOT NULL,
    bestshare     bigint DEFAULT 0         NOT NULL,
    CONSTRAINT user_workers_pk PRIMARY KEY (workername, pool_instance)
);

CREATE INDEX user_workers_address_idx ON user_workers (address, pool_instance);

-- DEDUIT (cf. en-tête)
CREATE TABLE share_weights_hourly
(
    workername text             NOT NULL,
    hour       timestamptz      NOT NULL,
    shares     bigint           NOT NULL,
    diff_sum   double precision NOT NULL,
    PRIMARY KEY (workername, hour)
);

-- DEDUIT (cf. en-tête)
CREATE TABLE worker_stats_1d
(
    day            date             NOT NULL,
    workername     text             NOT NULL,
    avg_hashrate1h double precision NOT NULL,
    PRIMARY KEY (day, workername)
);
