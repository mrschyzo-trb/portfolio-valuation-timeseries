create keyspace if not exists pv
    with replication = {'class':'SimpleStrategy', 'replication_factor':1};

create table if not exists pv.portfolio_valuation(
    user_id uuid,
    resolution text,
    time timestamp,
    amount decimal,
    primary key ((user_id, resolution), time)
) with clustering order by (time desc)
