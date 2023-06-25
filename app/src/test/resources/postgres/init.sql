create extension if not exists "uuid-ossp";

create table if not exists portfolio_valuation(
    user_id uuid not null,
    amount numeric not null,
    "timestamp" timestamptz not null,
    resolution smallint not null -- 1 = 10m, 2 = 1h, 3 = 4h, 4 = 1d, 5 = 1w
) partition by hash(user_id);

create index if not exists valuation_search_idx on portfolio_valuation (user_id, resolution, "timestamp");

create table portfolio_valuation_0 partition of portfolio_valuation for values with (modulus 32, remainder 0);
create table portfolio_valuation_1 partition of portfolio_valuation for values with (modulus 32, remainder 1);
create table portfolio_valuation_2 partition of portfolio_valuation for values with (modulus 32, remainder 2);
create table portfolio_valuation_3 partition of portfolio_valuation for values with (modulus 32, remainder 3);
create table portfolio_valuation_4 partition of portfolio_valuation for values with (modulus 32, remainder 4);
create table portfolio_valuation_5 partition of portfolio_valuation for values with (modulus 32, remainder 5);
create table portfolio_valuation_6 partition of portfolio_valuation for values with (modulus 32, remainder 6);
create table portfolio_valuation_7 partition of portfolio_valuation for values with (modulus 32, remainder 7);
create table portfolio_valuation_8 partition of portfolio_valuation for values with (modulus 32, remainder 8);
create table portfolio_valuation_9 partition of portfolio_valuation for values with (modulus 32, remainder 9);
create table portfolio_valuation_10 partition of portfolio_valuation for values with (modulus 32, remainder 10);
create table portfolio_valuation_11 partition of portfolio_valuation for values with (modulus 32, remainder 11);
create table portfolio_valuation_12 partition of portfolio_valuation for values with (modulus 32, remainder 12);
create table portfolio_valuation_13 partition of portfolio_valuation for values with (modulus 32, remainder 13);
create table portfolio_valuation_14 partition of portfolio_valuation for values with (modulus 32, remainder 14);
create table portfolio_valuation_15 partition of portfolio_valuation for values with (modulus 32, remainder 15);
create table portfolio_valuation_16 partition of portfolio_valuation for values with (modulus 32, remainder 16);
create table portfolio_valuation_17 partition of portfolio_valuation for values with (modulus 32, remainder 17);
create table portfolio_valuation_18 partition of portfolio_valuation for values with (modulus 32, remainder 18);
create table portfolio_valuation_19 partition of portfolio_valuation for values with (modulus 32, remainder 19);
create table portfolio_valuation_20 partition of portfolio_valuation for values with (modulus 32, remainder 20);
create table portfolio_valuation_21 partition of portfolio_valuation for values with (modulus 32, remainder 21);
create table portfolio_valuation_22 partition of portfolio_valuation for values with (modulus 32, remainder 22);
create table portfolio_valuation_23 partition of portfolio_valuation for values with (modulus 32, remainder 23);
create table portfolio_valuation_24 partition of portfolio_valuation for values with (modulus 32, remainder 24);
create table portfolio_valuation_25 partition of portfolio_valuation for values with (modulus 32, remainder 25);
create table portfolio_valuation_26 partition of portfolio_valuation for values with (modulus 32, remainder 26);
create table portfolio_valuation_27 partition of portfolio_valuation for values with (modulus 32, remainder 27);
create table portfolio_valuation_28 partition of portfolio_valuation for values with (modulus 32, remainder 28);
create table portfolio_valuation_29 partition of portfolio_valuation for values with (modulus 32, remainder 29);
create table portfolio_valuation_30 partition of portfolio_valuation for values with (modulus 32, remainder 30);
create table portfolio_valuation_31 partition of portfolio_valuation for values with (modulus 32, remainder 31);
