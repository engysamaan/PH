-- -- <AddendumII-Product> Only SQL
DECLARE @ProfID int;
SET @ProfID = 805;
--  1-  Customer Name
select AuthCodeIds, string_agg(ShipDistNumber,',') as CustomerNumber
from (Select ProfileLocation.ShipDistNumber,string_agg(AuthCodeID,',') as AuthCodeIDs from locationproducts
        join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
        where locationproducts.profileid=@ProfID and ProfileLocation.HideLocFromContract=0
        group by ProfileLocation.ShipDistNumber,LocationProducts.LocationID) as tt
    group by  AuthCodeIds

-- Locations:
select AuthCodeIds,string_agg(ShipDistNumber,',') as CS , string_agg(LocationID,',') as Locations
from (Select ProfileLocation.ShipDistNumber,locationproducts.LocationID, string_agg(AuthCodeID,',') as AuthCodeIDs from locationproducts
        join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
        where locationproducts.profileid=@ProfID and ProfileLocation.HideLocFromContract=0
        group by ProfileLocation.ShipDistNumber,LocationProducts.LocationID) as tt
    group by  AuthCodeIds
-- --  AuthCodeIds
-- Select ProfileLocation.ShipDistNumber, string_agg(AuthCodeID,',') as AuthCodeIDs
-- from locationproducts
--         join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
--         where locationproducts.profileid=806 and ProfileLocation.HideLocFromContract=0
--         group by ProfileLocation.ShipDistNumber

-- ?test
select top(1) AuthCodeID,ProfileLocation.ShipDistNumber, LocationProducts.LocationID  from LocationProducts
join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
-- 166917  | 166500
where locationproducts.profileid=806 and ProfileLocation.HideLocFromContract=0 and ProfileLocation.ShipDistNumber = 'L10191'


select top(1) AuthCodeID,ProfileLocation.ShipDistNumber, LocationProducts.LocationID  from LocationProducts
join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
-- 166917  | 166500
where locationproducts.profileid=806 and ProfileLocation.HideLocFromContract=0 and ProfileLocation.ShipDistNumber = '166917'


-- 2 - <AddendumII-ProductDetails>
##-- TODO: if Discount is Null then (-) Done

DECLARE @ID int;
SET @ID = 53;
select Distinct ProdDivPrint as Division,
AuthCodeMaster.Category as ProductAuthorizations,
LocationProducts.Classfication as 'AddendumII-Product_Classification',
LocationProducts.DiscountLevel as 'AddendumII-Product_Pricing'
from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--   ANY| Top LocationID and for EACH AuthCodeID
where ProfileID = 806 and LocationID =1404 and AuthCodeMaster.AuthCodeId = @ID

--  lop AUthCODE ID:
-- 3 Addendum Products:
select ProdCode ,ProdCodeMaster.ProdName  from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--    TOP LOCATION ID  & For EACH AUthCodeID with
where ProfileID = 806 and LocationID =1406 and AuthCodeMaster.AuthCodeId = @ID
