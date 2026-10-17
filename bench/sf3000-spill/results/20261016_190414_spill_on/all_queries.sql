CREATE VIEW customer AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/customer/customer.1.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.10.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.2.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.3.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.4.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.5.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.6.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.7.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.8.parquet','/scratch/sirius/datasets/tpch_sf3000/customer/customer.9.parquet']);
CREATE VIEW lineitem AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.1.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.10.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.100.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.101.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.102.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.103.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.104.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.105.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.106.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.107.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.108.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.109.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.11.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.110.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.111.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.112.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.113.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.114.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.115.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.116.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.117.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.118.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.119.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.12.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.120.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.121.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.122.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.123.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.124.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.125.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.126.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.127.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.128.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.129.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.13.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.130.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.131.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.132.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.133.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.134.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.135.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.136.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.137.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.138.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.139.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.14.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.140.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.141.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.142.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.143.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.144.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.145.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.146.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.147.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.148.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.149.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.15.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.150.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.151.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.152.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.153.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.154.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.155.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.156.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.157.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.158.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.159.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.16.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.160.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.161.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.162.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.163.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.164.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.165.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.166.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.167.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.168.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.169.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.17.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.170.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.171.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.172.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.173.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.174.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.175.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.176.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.177.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.178.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.179.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.18.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.180.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.19.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.2.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.20.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.21.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.22.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.23.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.24.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.25.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.26.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.27.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.28.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.29.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.3.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.30.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.31.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.32.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.33.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.34.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.35.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.36.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.37.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.38.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.39.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.4.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.40.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.41.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.42.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.43.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.44.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.45.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.46.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.47.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.48.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.49.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.5.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.50.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.51.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.52.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.53.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.54.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.55.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.56.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.57.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.58.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.59.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.6.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.60.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.61.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.62.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.63.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.64.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.65.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.66.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.67.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.68.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.69.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.7.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.70.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.71.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.72.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.73.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.74.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.75.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.76.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.77.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.78.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.79.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.8.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.80.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.81.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.82.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.83.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.84.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.85.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.86.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.87.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.88.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.89.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.9.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.90.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.91.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.92.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.93.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.94.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.95.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.96.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.97.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.98.parquet','/scratch/sirius/datasets/tpch_sf3000/lineitem/lineitem.99.parquet']);
CREATE VIEW nation AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/nation/nation.1.parquet']);
CREATE VIEW orders AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/orders/orders.1.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.10.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.11.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.12.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.13.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.14.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.15.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.16.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.17.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.18.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.19.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.2.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.20.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.21.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.22.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.23.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.24.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.25.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.26.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.27.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.28.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.29.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.3.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.30.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.31.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.32.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.33.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.34.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.35.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.36.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.37.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.38.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.39.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.4.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.40.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.41.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.42.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.43.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.44.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.45.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.46.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.47.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.48.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.49.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.5.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.50.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.51.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.52.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.6.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.7.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.8.parquet','/scratch/sirius/datasets/tpch_sf3000/orders/orders.9.parquet']);
CREATE VIEW part AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/part/part.1.parquet','/scratch/sirius/datasets/tpch_sf3000/part/part.2.parquet','/scratch/sirius/datasets/tpch_sf3000/part/part.3.parquet','/scratch/sirius/datasets/tpch_sf3000/part/part.4.parquet','/scratch/sirius/datasets/tpch_sf3000/part/part.5.parquet']);
CREATE VIEW partsupp AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.1.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.10.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.11.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.12.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.13.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.14.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.15.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.16.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.17.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.18.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.19.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.2.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.20.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.21.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.22.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.23.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.24.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.25.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.26.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.27.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.28.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.29.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.3.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.30.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.31.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.32.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.33.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.4.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.5.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.6.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.7.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.8.parquet','/scratch/sirius/datasets/tpch_sf3000/partsupp/partsupp.9.parquet']);
CREATE VIEW region AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/region/region.1.parquet']);
CREATE VIEW supplier AS SELECT * FROM read_parquet(['/scratch/sirius/datasets/tpch_sf3000/supplier/supplier.1.parquet']);

.timer on
.print __TPCH_MARKER__ 1
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1995-08-19'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1995-08-19'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

.print __TPCH_MARKER__ 2
select
  s.s_acctbal,
  s.s_name,
  n.n_name,
  p.p_partkey,
  p.p_mfgr,
  s.s_address,
  s.s_phone,
  s.s_comment
from
  part p,
  supplier s,
  partsupp ps,
  nation n,
  region r
where
  p.p_partkey = ps.ps_partkey
  and s.s_suppkey = ps.ps_suppkey
  and p.p_size = 41
  and p.p_type like '%NICKEL'
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and ps.ps_supplycost = (
    select
      min(ps.ps_supplycost)
    from
      partsupp ps,
      supplier s,
      nation n,
      region r
    where
      p.p_partkey = ps.ps_partkey
      and s.s_suppkey = ps.ps_suppkey
      and s.s_nationkey = n.n_nationkey
      and n.n_regionkey = r.r_regionkey
      and r.r_name = 'EUROPE'
  )
order by
  s.s_acctbal desc,
  n.n_name,
  s.s_name,
  p.p_partkey
limit 100;

select
  s.s_acctbal,
  s.s_name,
  n.n_name,
  p.p_partkey,
  p.p_mfgr,
  s.s_address,
  s.s_phone,
  s.s_comment
from
  part p,
  supplier s,
  partsupp ps,
  nation n,
  region r
where
  p.p_partkey = ps.ps_partkey
  and s.s_suppkey = ps.ps_suppkey
  and p.p_size = 41
  and p.p_type like '%NICKEL'
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and ps.ps_supplycost = (
    select
      min(ps.ps_supplycost)
    from
      partsupp ps,
      supplier s,
      nation n,
      region r
    where
      p.p_partkey = ps.ps_partkey
      and s.s_suppkey = ps.ps_suppkey
      and s.s_nationkey = n.n_nationkey
      and n.n_regionkey = r.r_regionkey
      and r.r_name = 'EUROPE'
  )
order by
  s.s_acctbal desc,
  n.n_name,
  s.s_name,
  p.p_partkey
limit 100;

.print __TPCH_MARKER__ 3
select
  l.l_orderkey,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  o.o_orderdate,
  o.o_shippriority
from
  customer c,
  orders o,
  lineitem l
where
  c.c_mktsegment = 'HOUSEHOLD'
  and c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate < date '1995-03-25'
  and l.l_shipdate > date '1995-03-25'
group by
  l.l_orderkey,
  o.o_orderdate,
  o.o_shippriority
order by
  revenue desc,
  o.o_orderdate
limit 10;

select
  l.l_orderkey,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  o.o_orderdate,
  o.o_shippriority
from
  customer c,
  orders o,
  lineitem l
where
  c.c_mktsegment = 'HOUSEHOLD'
  and c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate < date '1995-03-25'
  and l.l_shipdate > date '1995-03-25'
group by
  l.l_orderkey,
  o.o_orderdate,
  o.o_shippriority
order by
  revenue desc,
  o.o_orderdate
limit 10;

.print __TPCH_MARKER__ 4
select
  o.o_orderpriority,
  count(*) as order_count
from
  orders o
where
  o.o_orderdate >= date '1996-10-01'
  and o.o_orderdate < date '1997-01-01'
  and
  exists (
    select
      *
    from
      lineitem l
    where
      l.l_orderkey = o.o_orderkey
      and l.l_commitdate < l.l_receiptdate
  )
group by
  o.o_orderpriority
order by
  o.o_orderpriority;

select
  o.o_orderpriority,
  count(*) as order_count
from
  orders o
where
  o.o_orderdate >= date '1996-10-01'
  and o.o_orderdate < date '1997-01-01'
  and
  exists (
    select
      *
    from
      lineitem l
    where
      l.l_orderkey = o.o_orderkey
      and l.l_commitdate < l.l_receiptdate
  )
group by
  o.o_orderpriority
order by
  o.o_orderpriority;

.print __TPCH_MARKER__ 5
select
  n.n_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
from
  orders o,
  lineitem l,
  supplier s,
  nation n,
  region r,
  customer c
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and l.l_suppkey = s.s_suppkey
  and c.c_nationkey = s.s_nationkey
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and o.o_orderdate >= date '1997-01-01'
  and o.o_orderdate < date '1998-01-01'
group by
  n.n_name
order by
  revenue desc;

select
  n.n_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
from
  orders o,
  lineitem l,
  supplier s,
  nation n,
  region r,
  customer c
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and l.l_suppkey = s.s_suppkey
  and c.c_nationkey = s.s_nationkey
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and o.o_orderdate >= date '1997-01-01'
  and o.o_orderdate < date '1998-01-01'
group by
  n.n_name
order by
  revenue desc;

.print __TPCH_MARKER__ 6
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1997-01-01'
  and l_shipdate < date '1998-01-01'
  and l_discount between 0.03 - 0.01 and 0.03 + 0.01
  and l_quantity < 24;

select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1997-01-01'
  and l_shipdate < date '1998-01-01'
  and l_discount between 0.03 - 0.01 and 0.03 + 0.01
  and l_quantity < 24;

.print __TPCH_MARKER__ 7
select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(year from l.l_shipdate) as l_year,
      l.l_extendedprice * (1 - l.l_discount) as volume
    from
      supplier s,
      lineitem l,
      orders o,
      customer c,
      nation n1,
      nation n2
    where
      s.s_suppkey = l.l_suppkey
      and o.o_orderkey = l.l_orderkey
      and c.c_custkey = o.o_custkey
      and s.s_nationkey = n1.n_nationkey
      and c.c_nationkey = n2.n_nationkey
      and (
        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')
        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')
      )
      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
  ) as shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
  supp_nation,
  cust_nation,
  l_year;

select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (
    select
      n1.n_name as supp_nation,
      n2.n_name as cust_nation,
      extract(year from l.l_shipdate) as l_year,
      l.l_extendedprice * (1 - l.l_discount) as volume
    from
      supplier s,
      lineitem l,
      orders o,
      customer c,
      nation n1,
      nation n2
    where
      s.s_suppkey = l.l_suppkey
      and o.o_orderkey = l.l_orderkey
      and c.c_custkey = o.o_custkey
      and s.s_nationkey = n1.n_nationkey
      and c.c_nationkey = n2.n_nationkey
      and (
        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')
        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')
      )
      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'
  ) as shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
  supp_nation,
  cust_nation,
  l_year;

.print __TPCH_MARKER__ 8
select
  o_year,
  sum(case
    when nation = 'EGYPT' then volume
    else 0
  end) / sum(volume) as mkt_share
from
  (
    select
      extract(year from o.o_orderdate) as o_year,
      l.l_extendedprice * (1 - l.l_discount) as volume,
      n2.n_name as nation
    from
      lineitem l,
      part p,
      supplier s,
      orders o,
      customer c,
      nation n1,
      nation n2,
      region r
    where
      p.p_partkey = l.l_partkey
      and s.s_suppkey = l.l_suppkey
      and l.l_orderkey = o.o_orderkey
      and o.o_custkey = c.c_custkey
      and c.c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r.r_regionkey
      and r.r_name = 'MIDDLE EAST'
      and s.s_nationkey = n2.n_nationkey
      and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p.p_type = 'PROMO BRUSHED COPPER'
  ) as all_nations
group by
  o_year
order by
  o_year;

select
  o_year,
  sum(case
    when nation = 'EGYPT' then volume
    else 0
  end) / sum(volume) as mkt_share
from
  (
    select
      extract(year from o.o_orderdate) as o_year,
      l.l_extendedprice * (1 - l.l_discount) as volume,
      n2.n_name as nation
    from
      lineitem l,
      part p,
      supplier s,
      orders o,
      customer c,
      nation n1,
      nation n2,
      region r
    where
      p.p_partkey = l.l_partkey
      and s.s_suppkey = l.l_suppkey
      and l.l_orderkey = o.o_orderkey
      and o.o_custkey = c.c_custkey
      and c.c_nationkey = n1.n_nationkey
      and n1.n_regionkey = r.r_regionkey
      and r.r_name = 'MIDDLE EAST'
      and s.s_nationkey = n2.n_nationkey
      and o.o_orderdate between date '1995-01-01' and date '1996-12-31'
      and p.p_type = 'PROMO BRUSHED COPPER'
  ) as all_nations
group by
  o_year
order by
  o_year;

.print __TPCH_MARKER__ 9
select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n.n_name as nation,
      extract(year from o.o_orderdate) as o_year,
      l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
    from
      part p,
      supplier s,
      lineitem l,
      partsupp ps,
      orders o,
      nation n
    where
      s.s_suppkey = l.l_suppkey
      and ps.ps_suppkey = l.l_suppkey
      and ps.ps_partkey = l.l_partkey
      and p.p_partkey = l.l_partkey
      and o.o_orderkey = l.l_orderkey
      and s.s_nationkey = n.n_nationkey
      and p.p_name like '%yellow%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

select
  nation,
  o_year,
  sum(amount) as sum_profit
from
  (
    select
      n.n_name as nation,
      extract(year from o.o_orderdate) as o_year,
      l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount
    from
      part p,
      supplier s,
      lineitem l,
      partsupp ps,
      orders o,
      nation n
    where
      s.s_suppkey = l.l_suppkey
      and ps.ps_suppkey = l.l_suppkey
      and ps.ps_partkey = l.l_partkey
      and p.p_partkey = l.l_partkey
      and o.o_orderkey = l.l_orderkey
      and s.s_nationkey = n.n_nationkey
      and p.p_name like '%yellow%'
  ) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;

.print __TPCH_MARKER__ 10
select
  c.c_custkey,
  c.c_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
from
  customer c,
  orders o,
  lineitem l,
  nation n
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate >= date '1994-03-01'
  and o.o_orderdate < date '1994-06-01'
  and l.l_returnflag = 'R'
  and c.c_nationkey = n.n_nationkey
group by
  c.c_custkey,
  c.c_name,
  c.c_acctbal,
  c.c_phone,
  n.n_name,
  c.c_address,
  c.c_comment
order by
  revenue desc
limit 20;

select
  c.c_custkey,
  c.c_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
from
  customer c,
  orders o,
  lineitem l,
  nation n
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate >= date '1994-03-01'
  and o.o_orderdate < date '1994-06-01'
  and l.l_returnflag = 'R'
  and c.c_nationkey = n.n_nationkey
group by
  c.c_custkey,
  c.c_name,
  c.c_acctbal,
  c.c_phone,
  n.n_name,
  c.c_address,
  c.c_comment
order by
  revenue desc
limit 20;

.print __TPCH_MARKER__ 11
select
  ps.ps_partkey,
  sum(ps.ps_supplycost * ps.ps_availqty) as value
from
  partsupp ps,
  supplier s,
  nation n
where
  ps.ps_suppkey = s.s_suppkey
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'GERMANY'
group by
  ps.ps_partkey having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
      select
        sum(ps.ps_supplycost * ps.ps_availqty) * 0.00001
      from
        partsupp ps,
        supplier s,
        nation n
      where
        ps.ps_suppkey = s.s_suppkey
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'GERMANY'
    )
order by
  value desc;

select
  ps.ps_partkey,
  sum(ps.ps_supplycost * ps.ps_availqty) as value
from
  partsupp ps,
  supplier s,
  nation n
where
  ps.ps_suppkey = s.s_suppkey
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'GERMANY'
group by
  ps.ps_partkey having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
      select
        sum(ps.ps_supplycost * ps.ps_availqty) * 0.00001
      from
        partsupp ps,
        supplier s,
        nation n
      where
        ps.ps_suppkey = s.s_suppkey
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'GERMANY'
    )
order by
  value desc;

.print __TPCH_MARKER__ 12
select
  l.l_shipmode,
  sum(case
    when o.o_orderpriority = '1-URGENT'
      or o.o_orderpriority = '2-HIGH'
      then 1
    else 0
  end) as high_line_count,
  sum(case
    when o.o_orderpriority <> '1-URGENT'
      and o.o_orderpriority <> '2-HIGH'
      then 1
    else 0
  end) as low_line_count
from
  orders o,
  lineitem l
where
  o.o_orderkey = l.l_orderkey
  and l.l_shipmode in ('TRUCK', 'REG AIR')
  and l.l_commitdate < l.l_receiptdate
  and l.l_shipdate < l.l_commitdate
  and l.l_receiptdate >= date '1994-01-01'
  and l.l_receiptdate < date '1995-01-01'
group by
  l.l_shipmode
order by
  l.l_shipmode;

select
  l.l_shipmode,
  sum(case
    when o.o_orderpriority = '1-URGENT'
      or o.o_orderpriority = '2-HIGH'
      then 1
    else 0
  end) as high_line_count,
  sum(case
    when o.o_orderpriority <> '1-URGENT'
      and o.o_orderpriority <> '2-HIGH'
      then 1
    else 0
  end) as low_line_count
from
  orders o,
  lineitem l
where
  o.o_orderkey = l.l_orderkey
  and l.l_shipmode in ('TRUCK', 'REG AIR')
  and l.l_commitdate < l.l_receiptdate
  and l.l_shipdate < l.l_commitdate
  and l.l_receiptdate >= date '1994-01-01'
  and l.l_receiptdate < date '1995-01-01'
group by
  l.l_shipmode
order by
  l.l_shipmode;

.print __TPCH_MARKER__ 13
select
  c_count,
  count(*) as custdist
from
  (
    select
      c.c_custkey,
      count(o.o_orderkey)
    from
      customer c
      left outer join orders o
        on c.c_custkey = o.o_custkey
        and o.o_comment not like '%special%requests%'
    group by
      c.c_custkey
  ) as orders (c_custkey, c_count)
group by
  c_count
order by
  custdist desc,
  c_count desc;

select
  c_count,
  count(*) as custdist
from
  (
    select
      c.c_custkey,
      count(o.o_orderkey)
    from
      customer c
      left outer join orders o
        on c.c_custkey = o.o_custkey
        and o.o_comment not like '%special%requests%'
    group by
      c.c_custkey
  ) as orders (c_custkey, c_count)
group by
  c_count
order by
  custdist desc,
  c_count desc;

.print __TPCH_MARKER__ 14
select
  100.00 * sum(case
    when p.p_type like 'PROMO%'
      then l.l_extendedprice * (1 - l.l_discount)
    else 0
  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
  lineitem l,
  part p
where
  l.l_partkey = p.p_partkey
  and l.l_shipdate >= date '1994-08-01'
  and l.l_shipdate < date '1994-09-01';

select
  100.00 * sum(case
    when p.p_type like 'PROMO%'
      then l.l_extendedprice * (1 - l.l_discount)
    else 0
  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
  lineitem l,
  part p
where
  l.l_partkey = p.p_partkey
  and l.l_shipdate >= date '1994-08-01'
  and l.l_shipdate < date '1994-09-01';

.print __TPCH_MARKER__ 15
with revenue_view as (
  select
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
  from
    lineitem
  where
    l_shipdate >= date '1993-05-01'
    and l_shipdate < date '1993-08-01'
  group by
    l_suppkey
)

select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  supplier s,
  revenue_view r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
      revenue_view
  )
order by
  s.s_suppkey;

with revenue_view as (
  select
    l_suppkey as supplier_no,
    sum(l_extendedprice * (1 - l_discount)) as total_revenue
  from
    lineitem
  where
    l_shipdate >= date '1993-05-01'
    and l_shipdate < date '1993-08-01'
  group by
    l_suppkey
)

select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  supplier s,
  revenue_view r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
      revenue_view
  )
order by
  s.s_suppkey;

.print __TPCH_MARKER__ 16
select
  p.p_brand,
  p.p_type,
  p.p_size,
  count(distinct ps.ps_suppkey) as supplier_cnt
from
  partsupp ps,
  part p
where
  p.p_partkey = ps.ps_partkey
  and p.p_brand <> 'Brand#21'
  and p.p_type not like 'MEDIUM PLATED%'
  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)
  and ps.ps_suppkey not in (
    select
      s.s_suppkey
    from
      supplier s
    where
      s.s_comment like '%Customer%Complaints%'
  )
group by
  p.p_brand,
  p.p_type,
  p.p_size
order by
  supplier_cnt desc,
  p.p_brand,
  p.p_type,
  p.p_size;

select
  p.p_brand,
  p.p_type,
  p.p_size,
  count(distinct ps.ps_suppkey) as supplier_cnt
from
  partsupp ps,
  part p
where
  p.p_partkey = ps.ps_partkey
  and p.p_brand <> 'Brand#21'
  and p.p_type not like 'MEDIUM PLATED%'
  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)
  and ps.ps_suppkey not in (
    select
      s.s_suppkey
    from
      supplier s
    where
      s.s_comment like '%Customer%Complaints%'
  )
group by
  p.p_brand,
  p.p_type,
  p.p_size
order by
  supplier_cnt desc,
  p.p_brand,
  p.p_type,
  p.p_size;

.print __TPCH_MARKER__ 17
select
  sum(l.l_extendedprice) / 7.0 as avg_yearly
from
  lineitem l,
  part p
where
  p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#13'
  and p.p_container = 'JUMBO CAN'
  and l.l_quantity < (
    select
      0.2 * avg(l2.l_quantity)
    from
      lineitem l2
    where
      l2.l_partkey = p.p_partkey
  );

select
  sum(l.l_extendedprice) / 7.0 as avg_yearly
from
  lineitem l,
  part p
where
  p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#13'
  and p.p_container = 'JUMBO CAN'
  and l.l_quantity < (
    select
      0.2 * avg(l2.l_quantity)
    from
      lineitem l2
    where
      l2.l_partkey = p.p_partkey
  );

.print __TPCH_MARKER__ 18
select
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice,
  sum(l.l_quantity)
from
  customer c,
  orders o,
  lineitem l
where
  o.o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c.c_custkey = o.o_custkey
  and o.o_orderkey = l.l_orderkey
group by
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice
order by
  o.o_totalprice desc,
  o.o_orderdate
limit 100;

select
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice,
  sum(l.l_quantity)
from
  customer c,
  orders o,
  lineitem l
where
  o.o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c.c_custkey = o.o_custkey
  and o.o_orderkey = l.l_orderkey
group by
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice
order by
  o.o_totalprice desc,
  o.o_orderdate
limit 100;

.print __TPCH_MARKER__ 19
select
  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
  lineitem l,
  part p
where
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#41'
    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l.l_quantity >= 2 and l.l_quantity <= 2 + 10
    and p.p_size between 1 and 5
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#13'
    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l.l_quantity >= 14 and l.l_quantity <= 14 + 10
    and p.p_size between 1 and 10
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#55'
    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l.l_quantity >= 23 and l.l_quantity <= 23 + 10
    and p.p_size between 1 and 15
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  );

select
  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
  lineitem l,
  part p
where
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#41'
    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l.l_quantity >= 2 and l.l_quantity <= 2 + 10
    and p.p_size between 1 and 5
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#13'
    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l.l_quantity >= 14 and l.l_quantity <= 14 + 10
    and p.p_size between 1 and 10
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_partkey = l.l_partkey
    and p.p_brand = 'Brand#55'
    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l.l_quantity >= 23 and l.l_quantity <= 23 + 10
    and p.p_size between 1 and 15
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  );

.print __TPCH_MARKER__ 20
select
  s.s_name,
  s.s_address
from
  supplier s,
  nation n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      partsupp ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          part p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          lineitem l
        where
          l.l_partkey = ps.ps_partkey
          and l.l_suppkey = ps.ps_suppkey
          and l.l_shipdate >= date '1993-01-01'
          and l.l_shipdate < date '1994-01-01'
      )
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'KENYA'
order by
  s.s_name;

select
  s.s_name,
  s.s_address
from
  supplier s,
  nation n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      partsupp ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          part p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          lineitem l
        where
          l.l_partkey = ps.ps_partkey
          and l.l_suppkey = ps.ps_suppkey
          and l.l_shipdate >= date '1993-01-01'
          and l.l_shipdate < date '1994-01-01'
      )
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'KENYA'
order by
  s.s_name;

.print __TPCH_MARKER__ 21
select
  s.s_name,
  count(*) as numwait
from
  supplier s,
  lineitem l1,
  orders o,
  nation n
where
  s.s_suppkey = l1.l_suppkey
  and o.o_orderkey = l1.l_orderkey
  and o.o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'BRAZIL'
group by
  s.s_name
order by
  numwait desc,
  s.s_name
limit 100;

select
  s.s_name,
  count(*) as numwait
from
  supplier s,
  lineitem l1,
  orders o,
  nation n
where
  s.s_suppkey = l1.l_suppkey
  and o.o_orderkey = l1.l_orderkey
  and o.o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      lineitem l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      lineitem l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'BRAZIL'
group by
  s.s_name
order by
  numwait desc,
  s.s_name
limit 100;

.print __TPCH_MARKER__ 22
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(c_phone from 1 for 2) as cntrycode,
      c_acctbal
    from
      customer c
    where
      substring(c_phone from 1 for 2) in
        ('24', '31', '11', '16', '21', '20', '34')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(c_phone from 1 for 2) in
            ('24', '31', '11', '16', '21', '20', '34')
      )
      and not exists (
        select
          *
        from
          orders o
        where
          o.o_custkey = c.c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
    select
      substring(c_phone from 1 for 2) as cntrycode,
      c_acctbal
    from
      customer c
    where
      substring(c_phone from 1 for 2) in
        ('24', '31', '11', '16', '21', '20', '34')
      and c_acctbal > (
        select
          avg(c_acctbal)
        from
          customer
        where
          c_acctbal > 0.00
          and substring(c_phone from 1 for 2) in
            ('24', '31', '11', '16', '21', '20', '34')
      )
      and not exists (
        select
          *
        from
          orders o
        where
          o.o_custkey = c.c_custkey
      )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;

.print __TPCH_END__
