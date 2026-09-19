@sourcedata=EXTRACT cid int,cname string,ccontact long,clocation string
FROM "adl://cloudpandithadlsgen1.azuredatalakestore.net/cloudpandithdata/input/usqldata.csv"
USING Extractors.Csv(skipFirstNRows : 1);

@outputdata = SELECT *,CASE WHEN cid%2==0 THEN "yes" ELSE "No" END AS isdeveloper FROM @sourcedata;

OUTPUT @outputdata
TO "adl://cloudpandithadlsgen1.azuredatalakestore.net/cloudpandithdata/output/adfoutput.csv"
USING Outputters.Csv(outputHeader :true);