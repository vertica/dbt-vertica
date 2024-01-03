CREATE SCHEMA raw_schema;
CREATE TABLE raw_schema.states
(
    statename varchar(80) NOT NULL,
    statecode varchar(2) NOT NULL,
    region varchar(80) NOT NULL,
    division varchar(80) NOT NULL
);

CREATE TABLE raw_schema.store_orders
(
    productid int,
    productversion int,
    storeid int,
    vendorid int,
    employeeid int,
    ordernumber int,
    dateordered date,
    dateshipped date,
    expecteddeliverydate date,
    datedelivered date,
    qtyordered int,
    qtydelivered int,
    shippername varchar(32),
    unitprice int,
    shippingcost int,
    qtyinstock int,
    reorderlevel int,
    overstockceiling int,
    loadedattimestamp timestamptz
);

CREATE TABLE raw_schema.vendors
(
    vendorid int,
    vendorname varchar(64),
    vendoraddress varchar(64),
    vendorcity varchar(64),
    vendorstate char(2),
    lastdealupdate date,
    loadedattimestamp timestamptz
);