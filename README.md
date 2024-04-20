The Challenge: Data Warehouse and Data Pipeline
Challenge assets are available at:

The above includes a data folder that has several pipe delimited gzipped files of raw
data. The names of the files start with either hier or fact to signify whether they have
hierarchy (dimension) or fact data. The word following hier or fact indicates the table
name for the raw data. Each file has a header row with column names.
The hier files have id and label columns for each level in the hierarchy. For the most
part you can assume that the left most column is the primary key, but you should
ensure that you draw out a proper structure by looking at the many-to-one relationships
that the data manifests.
1. You must draw out an ER diagram showing raw table structure and any
relationships between them that you can infer using column names. You may
use schema inference tools, but you must document what you used and why.
You must add the final ER diagram and any documentation explaining it to your
submission’s Github repository.
2. You must build a pipeline that
a. Loads this raw data into the data warehouse from external storage such
as Azure Blobs, AWS S3 or the like. You must write basic checks such as
non-null, uniqueness of primary key, data types. Also check for foreign
key constraints between fact and dimension tables. Do it for at least one
hier (dimension), and one fact table.
b. Create a staging schema where the hierarchy table has been normalized
into a table for each level and the staged fact table has foreign key
relationships with those tables.
c. Create a refined table called mview_weekly_sales which aggregates
sales_units, sales_dollars, and disocunt_dollars by pos_site_id, sku_id,
fsclwk_id, price_substate_id and type.


#SOLUTION

1. INPUT DATA FILES ARE STORED ON S3 BUCKET "salesdataforgsde2404".
below are the file names
fact.averagecosts.dlm.gz
fact.transactions.dlm.gz
hier.clnd.dlm.gz
hier.hldy.dlm.gz
hier.invloc.dlm.gz
hier.invstatus.dlm.gz
hier.possite.dlm.gz
hier.pricestate.dlm.gz
hier.prod.dlm.gz
hier.rtlloc.dlm.gz

2. CREATE DATAFRAME FOR EACH FILE AND LOAD THE DATA ON DATAFRAME FOR FURTHER ANALYSIS
3. CHECK FOR NON-NULL VALUES
4. CHECK FOR UNIQUE KEY CONSTRAINS 
5. CHECK FOR THE DATATYPE FOR THE FIELDS
6. ONCE WE HAVE JOIN THE DATAFRAME TRANSSACTION AND CLND
7. AGGREATE DATA AND STORE THE DATA IN OUTPUT FOLDER

CHECK THE ERD DIAGRAM FOLDER


