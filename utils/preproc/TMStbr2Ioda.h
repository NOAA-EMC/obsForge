#pragma once

#include <algorithm>
#include <iostream>
#include <limits>
#include <netcdf>    // NOLINT (using C API)
#include <string>
#include <vector>

#include "eckit/config/LocalConfiguration.h"

#include <Eigen/Dense>    // NOLINT

#include "ioda/Group.h"
#include "ioda/ObsGroup.h"

#include "NetCDFToIodaConverter.h"

using netCDF::exceptions::NcException;

namespace obsforge {
  
  class TMStbr2Ioda : public NetCDFToIodaConverter {
   public:
    explicit TMStbr2Ioda(const eckit::Configuration & fullConfig, const eckit::mpi::Comm & comm)
      : NetCDFToIodaConverter(fullConfig, comm) {
      variable_ = "brightnessTemperature";
    }

    // Read netcdf file and populate iodaVars
    obsforge::preproc::iodavars::IodaVars providerToIodaVars(const std::string fileName) final {
      oops::Log::info() << "Processing files provided by TMSTBR" << std::endl;

    // Try to open the NetCDF file in read-only mode. If failed, return empty iodaVars object
      try {
        netCDF::NcFile ncFile(fileName, netCDF::NcFile::read);
      } catch (const NcException &e) {
        oops::Log::warning() << "Warning: Failed to read file " << fileName << ". Skipping." << std::endl;
        oops::Log::warning() << e.what() << std::endl;
        obsforge::preproc::iodavars::IodaVars iodaVars(0, {}, {});
        return iodaVars;
      }

      // Open the NetCDF file in read-only mode
      netCDF::NcFile ncFile(fileName, netCDF::NcFile::read);
      oops::Log::info() << "Reading... " << fileName << std::endl;
      // Get dimensions
      int dim0 = ncFile.getDim("spots").getSize();
      int dim1 = ncFile.getDim("scans").getSize();
      int dim2 = ncFile.getDim("channels").getSize();
      oops::Log::info() << "spots, scans, channels: " << dim0 << dim1 << dim2 << std::endl;

      // Read lat, lon and brightness temp (TBR)
      float lon2d[dim0][dim1];
      ncFile.getVar("longitude").getVar(lon2d);

      float lat2d[dim0][dim1];
      ncFile.getVar("latitude").getVar(lat2d);

// Read SST ObsValue
      std::vector<int16_t> sstObsVal(dimTime*dimLat*dimLon);
      ncFile.getVar("sea_surface_temperature").getVar(sstObsVal.data());


      float TBR[dim0][dim1][dim2];
      ncFile.getVar("brightness_temperature").getVar(var);

      const fillValue = btVar.getAtt("_FillValue");
      // float fillValue;
      // NcVarAtt fillAtt = btVar.getAtt("_FillValue");

      // Read combinedQualityFlag
      int8_t comQcF[dim0][dim1][dim2];
      ncFile.getVar("combinedQualityFlag").getVar(comQcF);

      // Read time and convert secondsSince 1970-01-01 00:00:00
      NcVar vYear = ncFile.getVar("Year")
      NcVar vMonth = ncFile.getVar("Month")
      NcVar vDay = ncFile.getVar("Day")
      NcVar vHour = ncFile.getVar("Hour")
      NcVar vMinute = ncFile.getVar("Minute")
      NcVar vSecond = ncFile.getVar("Second")
      // NcVar vMillisecond = ncFile.getVar("Millisecond")
    
      std::tm timeSinceRef = {};
      timeSinceRef.tm_year = vYear - 1970  // years since 1970
      timeSinceRef.tm_mon = vMonth - 1;    // months since January (months are 0-based)
      timeSinceRef.tm_mday = vDay - 1;   // days since 1st day of the month
      timeSinceRef.tm_hour = vHour;
      timeSinceRef.tm_min = vMinute;
      timeSinceRef.tm_sec = vSecond;

      std::time_t secondsSinceReference = timegm(&timeSinceRef)

      
      // Apply scaling/unit change and compute the necessary fields
      std::vector<float> obsvalue(dim0*dim1*dim2)
      TBR.getVar(obsvalue.data())

      std::vector<float> obserror(dim0*dim1*dim2)
      error.getVar(obserror.data())

      std::vector<float> preqc(dim0*dim1*dim2)
      comQcF.getVar(preqc.data())

      std::vector<float> lat(dim0*dim1)
      lat2d.getVar(lat.data())

      std::vector<float> lon(dim0*dim1)
      lon2d.getVar(lon.data())

      // Optional MetaData fields
      // sensor_view_angle
      // sensor_zenith_angle


      // read in channel number
      std::string channels;
      fullConfig_.get("channel", channels);
      std::istringstream ss(channels);
      std::vector<int> channelNumber;
      std::string substr;
      while (std::getline(ss, substr, ',')) {
         int intValue = std::stoi(substr);
         channelNumber.push_back(intValue);
      }
      oops::Log::info() << " channels " << channelNumber << std::endl;
      int nchan(channelNumber.size());
      oops::Log::info() << " number of channels " << nchan << std::endl;

      // Create instance of iodaVars object
      obsforge::preproc::iodavars::IodaVars iodaVars(nobs, {}, {});
      iodaVars.referenceDate_ = "seconds since 1970-01-01T00:00:00Z";

      oops::Log::info() << " eigen... dim0, dim1 and dim2 :" << obsvalue.size() << " "
                        << obsvalue[0].size() << std::endl;
      
      // Store into eigen arrays
      for (int k = 0; k < nchan; k++) {
          iodaVars.channelValues_(k) = channelNumber[k];
          int loc(0);
          for (int i = 0; i < dim1; i++) {
              for (int j = 0; j < dim0; j++) {
                 //if (mask_s[i][j] == 1) {                             // mask apply to all channels
                    iodaVars.longitude_(loc) = lon[loc];
                    iodaVars.latitude_(loc) = lat2d_s[i][j];
                    iodaVars.datetime_(loc) = secondsSinceReference;
                    // VIIRS AOD use only one channel (4)
                    iodaVars.obsVal_(nchan*loc+k) = obsvalue_s[i][j];
                    if ( fullConfig_.has("binning") ) {
                       iodaVars.preQc_(nchan*loc+k)     = 0;
                    } else {
                       iodaVars.preQc_(nchan*loc+k)     = preqc[i][j];
                    }
                    iodaVars.obsError_(nchan*loc+k) = obserror_s[i][j];
                    loc += 1;
                // }
              }
          }
          oops::Log::info() << " total location "  << loc << std::endl;
      }

