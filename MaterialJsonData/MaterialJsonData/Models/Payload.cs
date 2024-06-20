namespace MaterialJsonData.Models
{
    public class Payload
    {    
        public string id { get; set; }
        public string country { get; set; }
        public string productid { get; set; }
        public string productdesc { get; set; }
        public string packuom { get; set; }
        public string partsperpack { get; set; }
        public string groupcode { get; set; }
        public string groupdesc { get; set; }
        public string brandcode { get; set; }
        public string branddesc { get; set; }
        public string flavourcode { get; set; }
        public string flavourdesc { get; set; }
        public string parttype { get; set; }
        public string packsize { get; set; }
        public string baseprice { get; set; }
        public string productgroup { get; set; }
        public string plant { get; set; }
        public string MaterialType { get; set; }      
        public string PackType { get; set; }
        public string ExternalMaterialGroup { get; set; }
        public string MaterialHierarchyCode { get; set; }
        public string ServingSize { get; set; }
        public string convFactorDen { get; set; }
        public string convFactorNum { get; set; }
        public string IsActive { get; set; } = "1";
        public string SalesOrg { get; set; }
        public string DistributionChannel { get; set; }
        public string TaxClassMaterial { get; set; }
        public string MaterialPriceGroup { get; set; }
        public string ConsumerSku { get; set; }
        public string MaterialCategory { get; set; }
        public string FlavourId { get; set; }      
        public string BeverageMaterial { get;  set; }
        public string PackTypeId { get; set; }
        public string BrandId { get; set; }
    }
}
