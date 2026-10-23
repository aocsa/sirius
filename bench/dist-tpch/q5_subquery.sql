-- TPC-H Q5 written with partial aggregates after the lineitem join and after the supplier
-- join, and nation joined last. Region ASIA, 1994.
select n_name, revenue
from (
  select ol.c_nationkey, sum(ol.rev) as revenue
  from (
    select l_suppkey, c_nationkey, sum(l_extendedprice * (1 - l_discount)) as rev
    from orders, lineitem, (
      select c_custkey, c_nationkey from customer
      where c_nationkey in (select n_nationkey from nation, region
                            where n_regionkey = r_regionkey and r_name = 'ASIA')
    ) cust
    where o_custkey = c_custkey and l_orderkey = o_orderkey
      and o_orderdate >= date '1994-01-01' and o_orderdate < date '1995-01-01'
    group by l_suppkey, c_nationkey
  ) ol, supplier
  where ol.l_suppkey = s_suppkey and ol.c_nationkey = s_nationkey
  group by ol.c_nationkey
) by_nation, nation
where by_nation.c_nationkey = n_nationkey
order by revenue desc;
