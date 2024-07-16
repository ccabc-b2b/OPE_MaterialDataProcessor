using System;
using MaterialJsonData.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Data;
using ErrorEventArgs = Newtonsoft.Json.Serialization.ErrorEventArgs;
using System.Collections.Generic;
using System.Linq;

namespace MaterialJsonData
{
    public class MaterialJsonData
    {
        private readonly IConfiguration _configuration;
        readonly string containerName = Properties.Settings.Default.ContainerName;
        readonly string blobDirectoryPrefix = Properties.Settings.Default.BlobDirectoryPrefix;
        readonly string destblobDirectoryPrefix = Properties.Settings.Default.DestDirectory;
        public MaterialJsonData(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        public void LoadMaterialData()
        {
            try
            {
                List<BlobEntity> blobList = new List<BlobEntity>();
                var storageKey = _configuration["StorageKey"];
                var storageAccount = CloudStorageAccount.Parse(storageKey);
                var myClient = storageAccount.CreateCloudBlobClient();
                var container = myClient.GetContainerReference(containerName);
                var list = container.ListBlobs().OfType<CloudBlobDirectory>().ToList();
                var blobListDirectory = list[0].ListBlobs().OfType<CloudBlobDirectory>().ToList();            
                foreach (var blobDirectory in blobListDirectory)
                {
                    if (blobDirectory.Prefix == blobDirectoryPrefix)
                    {                     
                        foreach (var blobFile in blobDirectory.ListBlobs().OfType<CloudBlockBlob>())
                        {                         
                            BlobEntity blobDetails = new BlobEntity();
                            string[] blobName = blobFile.Name.Split(new char[] { '/' });
                            string[] filename = blobName[2].Split(new char[] { '.' });
                            string[] fileDateTime = filename[0].Split(new char[] { '_' });
                            string fileCreatedDateTime = fileDateTime[1] + fileDateTime[2];
                            string formatString = "yyyyMMddHHmmss";
                            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobFile.Name);
                            blobDetails.Blob = blockBlob;
                            blobDetails.FileName = blobName[2];
                            blobDetails.FileCreatedDate = DateTime.ParseExact(fileCreatedDateTime, formatString, null);
                            blobDetails.FileData = blockBlob.DownloadTextAsync().Result;
                            blobDetails.BlobName = blobFile.Name;
                            blobList.Add(blobDetails);
                            
                        }
                        blobList.OrderByDescending(x => x.FileCreatedDate.Date).ThenByDescending(x => x.FileCreatedDate.TimeOfDay).ToList();
                    }
                }
                foreach (var blobDetails in blobList)
                {
                    CheckRequiredFields(blobDetails, container);
                }
            }
            catch (StorageException ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Material";
                errorLog.ErrorMessage = ex.Message;
                SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        private void CheckRequiredFields(BlobEntity blobDetails, CloudBlobContainer container)
        {
            try
            {
                List<string> errors = new List<string>();
                if (string.IsNullOrEmpty(blobDetails.FileData))
                {
                    blobDetails.Status = "Error";
                    var errorLog = new ErrorLogEntity();
                    errorLog.PipeLineName = "Material";
                    errorLog.FileName = blobDetails.FileName;
                    errorLog.ErrorMessage = "File is empty";
                    SaveErrorLogData(errorLog);
                    Logger logger = new Logger(_configuration);
                    logger.ErrorLogData(null, "File is empty");
                }
                else
                {
                    MaterialJsonEntity materialdataList = JsonConvert.DeserializeObject<MaterialJsonEntity>(blobDetails.FileData, new JsonSerializerSettings
                    {
                        Error = delegate (object sender, ErrorEventArgs args)
                        {
                            errors.Add(args.ErrorContext.Error.Message);
                            args.ErrorContext.Handled = true;

                        },
                        Converters = { new IsoDateTimeConverter() }
                    });
                    Dictionary<string, int> returnData = new Dictionary<string, int>();
                    if (materialdataList == null)
                    {
                        returnData.Add("Material", 0);
                        var errorLog = new ErrorLogEntity();
                        errorLog.PipeLineName = "Material";
                        errorLog.FileName = blobDetails.FileName;
                        errorLog.ErrorMessage = errors[0];
                        SaveErrorLogData(errorLog);
                        Logger logger = new Logger(_configuration);
                        logger.ErrorLogData(null, errors[0]);
                    }
                    else
                    {
                        int countMaterial = 0;
                        foreach (var payload in materialdataList.payload)
                        {
                            if (payload.productid == null)
                            {
                                returnData.Add("Product Id is null", 0);
                            }
                            else if (payload.partsperpack == null)
                            {
                                returnData.Add("PartsPerPack is null", 0);
                            }
                            else if (payload.productgroup == null)
                            {
                                returnData.Add("ProductGroup is null", 0);
                            }
                            else
                            {
                                countMaterial++;
                                var materialDBEntity = new MaterialDBEntity();
                                materialDBEntity.MaterialNumber = payload.productid;
                                materialDBEntity.PartsPerPack = payload.partsperpack;
                                materialDBEntity.MaterialType = payload.MaterialType; 
                                materialDBEntity.MaterialGroup = payload.productgroup;
                                materialDBEntity.BrandId = payload.BrandId;                               
                                materialDBEntity.BeverageMaterial = payload.BeverageMaterial;
                                materialDBEntity.PackTypeId = payload.PackTypeId;
                                materialDBEntity.FlavourId = payload.FlavourId;
                                materialDBEntity.MaterialCategory = payload.MaterialCategory;
                                materialDBEntity.ConsumerSku = payload.ConsumerSku;
                                materialDBEntity.MaterialPriceGroup = payload.MaterialPriceGroup;
                                materialDBEntity.TaxClassMaterial = payload.TaxClassMaterial;
                                materialDBEntity.SalesOrg = payload.SalesOrg;
                                materialDBEntity.DistributionChannel = payload.DistributionChannel;
                                materialDBEntity.IsActive = payload.IsActive;
                                materialDBEntity.PackSize = payload.packsize;
                                materialDBEntity.convFactorNum = payload.convFactorNum;
                                materialDBEntity.convFactorDen = payload.convFactorDen;
                                materialDBEntity.ServingSize = payload.ServingSize;
                                materialDBEntity.ExternalMaterialGroup = payload.ExternalMaterialGroup;
                                materialDBEntity.MaterialHierarchyCode = payload.MaterialHierarchyCode;
                                materialDBEntity.PackType = payload.PackType;
                                materialDBEntity.FlavourCode = payload.flavourcode;
                                materialDBEntity.BaseUOM = payload.packuom;
                                materialDBEntity.PlantID = payload.plant;                             
                                var return_Customer = SaveMaterialData(materialDBEntity);
                                returnData.Add("Material" + countMaterial, return_Customer);

                            }
                        }
                    }
                    foreach (var returnvalue in returnData)
                    {
                        if (returnvalue.Value == 0)
                        {
                            blobDetails.Status = "Error";
                            var errorLog2 = new ErrorLogEntity();
                            errorLog2.PipeLineName = "Material";
                            errorLog2.FileName = blobDetails.FileName;
                            errorLog2.ParentNodeName = returnvalue.Key;
                            SaveErrorLogData(errorLog2);
                            break;
                        }
                        else
                        {
                            blobDetails.Status = "Success";
                        }
                    }
                }
                var destDirectory = destblobDirectoryPrefix + DateTime.Now.Year + "/" + DateTime.Now.Month + "/" + DateTime.Now.Day;
                MoveFile(blobDetails, container, destDirectory);
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Material";
                errorLog.ParentNodeName = "CheckRequiredFields";
                errorLog.ErrorMessage = ex.Message;
                SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        private int SaveMaterialData(MaterialDBEntity materialdata)
        {
            try
            {
                SqlConnection con = new SqlConnection(_configuration["DatabaseConnectionString"]);
                SqlCommand cmd = new SqlCommand("Material_save", con);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@MaterialNumber", materialdata.MaterialNumber);
                cmd.Parameters.AddWithValue("@PartsPerPack", materialdata.PartsPerPack);
                cmd.Parameters.AddWithValue("@MaterialType", materialdata.MaterialType);
                cmd.Parameters.AddWithValue("@MaterialGroup", materialdata.MaterialGroup);
                cmd.Parameters.AddWithValue("@BrandId", materialdata.BrandId);               
                cmd.Parameters.AddWithValue("@BeverageMaterial", materialdata.BeverageMaterial);
                cmd.Parameters.AddWithValue("@PackTypeId", materialdata.PackTypeId);
                cmd.Parameters.AddWithValue("@FlavourId", materialdata.FlavourId);
                cmd.Parameters.AddWithValue("@MaterialCategory", materialdata.MaterialCategory);
                cmd.Parameters.AddWithValue("@ConsumerSku", materialdata.ConsumerSku);
                cmd.Parameters.AddWithValue("@MaterialPriceGroup", materialdata.MaterialPriceGroup);
                cmd.Parameters.AddWithValue("@TaxClassMaterial", materialdata.TaxClassMaterial);
                cmd.Parameters.AddWithValue("@SalesOrg", materialdata.SalesOrg);
                cmd.Parameters.AddWithValue("@DistributionChannel", materialdata.DistributionChannel);
                cmd.Parameters.AddWithValue("@IsActive", materialdata.IsActive);
                cmd.Parameters.AddWithValue("@PackSize", materialdata.PackSize);
                cmd.Parameters.AddWithValue("@convFactorNum", materialdata.convFactorNum);
                cmd.Parameters.AddWithValue("@convFactorDen", materialdata.convFactorDen);
                cmd.Parameters.AddWithValue("@ServingSize", materialdata.ServingSize);
                cmd.Parameters.AddWithValue("@ExternalMaterialGroup", materialdata.ExternalMaterialGroup);
                cmd.Parameters.AddWithValue("@MaterialHierarchyCode", materialdata.MaterialHierarchyCode);
                cmd.Parameters.AddWithValue("@PackType", materialdata.PackType);
                cmd.Parameters.AddWithValue("@FlavourCode", materialdata.FlavourCode);
                cmd.Parameters.AddWithValue("@BaseUOM", materialdata.BaseUOM);
                cmd.Parameters.AddWithValue("@PlantID ", materialdata.PlantID);              
                cmd.Parameters.Add("@returnObj", System.Data.SqlDbType.BigInt).Direction = System.Data.ParameterDirection.Output;
                con.Open();
                int retval = cmd.ExecuteNonQuery();
                con.Close();
                if (retval != 0)
                {
                    return retval;
                }
                else
                {
                    return 0;
                }
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Material";
                errorLog.ParentNodeName = "Material Save";
                errorLog.ErrorMessage = ex.Message;
                SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
            return 0;
        }
        public void MoveFile(BlobEntity blob, CloudBlobContainer destContainer, string destDirectory)
        {
            CloudBlockBlob destBlob;
            try
            {
                if (blob.Blob == null)
                    throw new Exception("Source blob cannot be null.");

                if (!destContainer.Exists())
                    throw new Exception("Destination container does not exist.");

                string name = blob.FileName;
                if (destDirectory != "" && blob.Status == "Success")
                    destBlob = destContainer.GetBlockBlobReference(destDirectory + "\\Success\\" + name);
                else
                    destBlob = destContainer.GetBlockBlobReference(destDirectory + "\\Error\\" + name);

                destBlob.StartCopy(blob.Blob);              
                blob.Blob.Delete();
            }
            catch (Exception ex)
            {
                var errorLog = new ErrorLogEntity();
                errorLog.PipeLineName = "Material";
                errorLog.FileName = blob.FileName;
                errorLog.ParentNodeName = "Material move";
                errorLog.ErrorMessage = ex.Message;
                SaveErrorLogData(errorLog);
                Logger logger = new Logger(_configuration);
                logger.ErrorLogData(ex, ex.Message);
            }
        }
        private void SaveErrorLogData(ErrorLogEntity errorLogData)
        {
            try
            {
                SqlConnection con = new SqlConnection(_configuration["DatabaseConnectionString"]);
                SqlCommand cmd = new SqlCommand("ErrorLogDetails_save", con);
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@PipeLineName", errorLogData.PipeLineName);
                cmd.Parameters.AddWithValue("@FileName", errorLogData.FileName);
                cmd.Parameters.AddWithValue("@ParentNodeName", errorLogData.ParentNodeName);
                cmd.Parameters.AddWithValue("@ErrorMessage", errorLogData.ErrorMessage);
                con.Open();
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (Exception)
            {

            }
        }
    }
}
