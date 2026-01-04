create database project3_db;

SELECT COUNT(*) FROM project3_db.dbo.date_dim;
SELECT COUNT(*) FROM project3_db.dbo.order_items;
SELECT COUNT(*) FROM project3_db.dbo.order_item_options;

SELECT TOP 5 * FROM project3_db.dbo.date_dim;
SELECT TOP 5 * FROM project3_db.dbo.order_item_options;
SELECT TOP 5 * FROM project3_db.dbo.order_items;