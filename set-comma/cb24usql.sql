@sourcedata=EXTRACT 
cid int,
cname string,
ccontact long,
clocation string
FROM "adl://cloudpandithstoragegen1.azuredatalakestore.net/input/usqldata.csv"
USING Extractors.Csv(skipFirstNRows :1);

@outputdata=SELECT *,CASE WHEN cid%2==0 THEN "No" ELSE "yes" END AS isdeveloper FROM @sourcedata;

OUTPUT @outputdata
TO "adl://cloudpandithstoragegen1.azuredatalakestore.net/adfoutput/usqldata.csv"
USING Outputters.Csv(outputHeader :true);