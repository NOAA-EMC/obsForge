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
#include "superob.h"   // NOLINT

using netCDF::exceptions::NcException;

namespace obsforge {

  class Viirsaod2Ioda : public NetCDFToIodaConverter {
   public:
    explicit Viirsaod2Ioda(const eckit::Configuration & fullConfig, const eckit::mpi::Comm & comm)
      : NetCDFToIodaConverter(fullConfig, comm) {
      variable_ = "aerosolOpticalDepth";
    }

    // Read netcdf file and populate iodaVars
    obsforge::preproc::iodavars::IodaVars providerToIodaVars(const std::string fileName) final {
      oops::Log::info() << "Processing files provided by VIIRSAOD" << std::endl;

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
      int dimRow = ncFile.getDim("Rows").getSize();
      int dimCol = ncFile.getDim("Columns").getSize();
      oops::Log::info() << "row,col " << dimRow << dimCol << std::endl;

      // Read lat and lon
      float lon2d[dimRow][dimCol];
      ncFile.getVar("Longitude").getVar(lon2d);

      float lat2d[dimRow][dimCol];
      ncFile.getVar("Latitude").getVar(lat2d);

      float aod550[dimRow][dimCol];
      ncFile.getVar("AOD550").getVar(aod550);
      const float missingValue = -999.999;

      int8_t qcall[dimRow][dimCol];
      ncFile.getVar("QCAll").getVar(qcall);

      int8_t qcpath[dimRow][dimCol];
      ncFile.getVar("QCPath").getVar(qcpath);

      // string obstime
      std::string time_coverage_end;
      ncFile.getAtt("time_coverage_end").getValues(time_coverage_end);
      oops::Log::info() << "time_coverage_end type " << time_coverage_end << std::endl;
      std::tm timeinfo = {};
      // Create an input string stream and parse the string
      std::istringstream dateStream(time_coverage_end);
      dateStream >> std::get_time(&timeinfo, "%Y-%m-%dT%H:%M:%SZ");

      std::tm referenceTime = {};
      referenceTime.tm_year = 70;  // 1970
      referenceTime.tm_mon = 0;    // January (months are 0-based)
      referenceTime.tm_mday = 1;   // 1st day of the month
      referenceTime.tm_hour = 0;
      referenceTime.tm_min = 0;
      referenceTime.tm_sec = 0;

      std::time_t timestamp = std::mktime(&timeinfo);
      std::time_t referenceTimestamp = std::mktime(&referenceTime);
      std::time_t secondsSinceReference = std::difftime(timestamp, referenceTimestamp);


      // Apply scaling/unit change and compute the necessary fields
      std::vector<std::vector<int>> mask(dimRow, std::vector<int>(dimCol));
      std::vector<std::vector<float>> obsvalue(dimRow, std::vector<float>(dimCol));
      std::vector<std::vector<float>> obsvalue_raw(dimRow, std::vector<float>(dimCol));
      std::vector<std::vector<float>> obserror(dimRow, std::vector<float>(dimCol));
      std::vector<std::vector<int>> preqc(dimRow, std::vector<int>(dimCol));
      std::vector<std::vector<float>> lat(dimRow, std::vector<float>(dimCol));
      std::vector<std::vector<float>> lon(dimRow, std::vector<float>(dimCol));
      std::vector<std::vector<float>> pathflag(dimRow, std::vector<float>(dimCol));


      // Thinning
      float thinThreshold;
      fullConfig_.get("thinning.threshold", thinThreshold);
      int preQcValue;
      fullConfig_.get("preqc", preQcValue);
      oops::Log::info() << " thinthreshold " << thinThreshold << std::endl;
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_real_distribution<> dis(0.0, 1.0);

      // Create thinning and missing value mask
      for (int i = 0; i < dimRow; i++) {
        for (int j = 0; j < dimCol; j++) {
          if (aod550[i][j] != missingValue && qcall[i][j] <= preQcValue) {
         // Random number generation for thinning
             float isThin = dis(gen);
             if (isThin > thinThreshold) {
                preqc[i][j] = static_cast<int>(qcall[i][j]);
                lat[i][j] = lat2d[i][j];
                lon[i][j] = lon2d[i][j];
                // dark land
                float obserrorValue1 = 0.111431 + 0.128699 * static_cast<float>(aod550[i][j]);  // (Huang et al. 2023)
		// Innovation-based diagnostic (Desroziers et al, 2005)
		float obserrorValue2 = -0.127 + 0.547 * static_cast<float>(aod550[i][j]);  
		float obsBiasValue = -0.014 + 0.158 * static_cast<float>(aod550[i][j]);        // (Huang et al. 2023)
		pathflag[i][j] = static_cast<float>(1);   // briefly assign dark land flag to 1

                // ocean
                if (qcpath[i][j] % 2 == 1) {
                    obserrorValue1 = 0.00784394 + 0.219923 * static_cast<float>(aod550[i][j]);
		    obserrorValue2 = -0.069 + 0.483 * static_cast<float>(aod550[i][j]);
		    obsBiasValue = -0.015 + 0.077 * static_cast<float>(aod550[i][j]);
		    pathflag[i][j] = static_cast<float>(0); // briefly assign ocean flag to 1
                }
                // bright land
                if (qcpath[i][j] % 4 == 2) {
                   obserrorValue1 = 0.0550472 + 0.299558 *  static_cast<float>(aod550[i][j]);
		   obserrorValue2 = -0.101 + 0.644 *  static_cast<float>(aod550[i][j]);
		   obsBiasValue = -0.011 + 0.150 * static_cast<float>(aod550[i][j]);
		   pathflag[i][j] = static_cast<float>(2); // briefly assign bright land flag to 2
                }

		obsvalue[i][j] = static_cast<float>(aod550[i][j]);
		obsvalue[i][j] -= obsBiasValue;
		obserror[i][j] = std::max(obserrorValue1,obserrorValue2);
                mask[i][j] = 1;
             }
          }
        }
      }

      std::vector<std::vector<float>> obsvalue_s;
      std::vector<std::vector<float>> lon2d_s;
      std::vector<std::vector<float>> lat2d_s;
      std::vector<std::vector<float>> obserror_s;
      std::vector<std::vector<int>> mask_s;
      std::vector<std::vector<float>> pathflag_s;

      if ( fullConfig_.has("binning") ) {
        // Do superobbing
        // Deal with longitude when points cross the international date line
        float minLon = std::numeric_limits<float>::max();
        float maxLon = std::numeric_limits<float>::min();

        for (const auto& row : lon) {
           minLon = std::min(minLon, *std::min_element(row.begin(), row.end()));
           maxLon = std::max(maxLon, *std::max_element(row.begin(), row.end()));
        }

        if (maxLon - minLon > 180) {
        // Normalize longitudes to the range [0, 360)
           for (auto& row : lon) {
               for (float& lonValue : row) {
                   lonValue = fmod(lonValue + 360, 360);
               }
           }
        }

        lon2d_s = obsforge::superobutils::subsample2D(lon, mask, fullConfig_);
        for (auto& row : lon2d_s) {
            for (float& lonValue : row) {
                lonValue = fmod(lonValue + 360, 360);
            }
        }

        lat2d_s = obsforge::superobutils::subsample2D(lat, mask, fullConfig_);
        mask_s = obsforge::superobutils::subsample2D(mask, mask, fullConfig_);
        pathflag_s = obsforge::superobutils::subsample2DMode(pathflag, mask, fullConfig_);
        if (fullConfig_.has("binning.cressman radius")) {
        // Weighted-average (cressman) superob
          bool useCressman = true;
          obsvalue_s = obsforge::superobutils::subsample2D(obsvalue, mask, fullConfig_,
                       useCressman, lat, lon, lat2d_s, lon2d_s);
          obserror_s = obsforge::superobutils::subsample2D(obserror, mask, fullConfig_,
                       useCressman, lat, lon, lat2d_s, lon2d_s);
          obsvalue_raw_s = obsforge::superobutils::subsample2D(obsvalue_raw, mask, fullConfig_,
                       useCressman, lat, lon, lat2d_s, lon2d_s);
        } else {
        // Simple-average superob
          obsvalue_s = obsforge::superobutils::subsample2D(obsvalue, mask, fullConfig_);
          obserror_s = obsforge::superobutils::subsample2D(obserror, mask, fullConfig_);
          obsvalue_raw_s = obsforge::superobutils::subsample2D(obsvalue_raw, mask, fullConfig_);
        }
      } else {
        obsvalue_s = obsvalue;
        obsvalue_raw_s = obsvalue_raw;
        lon2d_s = lon;
        lat2d_s = lat;
        obserror_s = obserror;
        mask_s = mask;
	pathflag_s = pathflag;
      }

      int dimRow_s = obsvalue_s.size();
      int dimCol_s = obsvalue_s[0].size();
      int nobs(0);
      for (int i = 0; i < dimRow_s; i++) {
        for (int j = 0; j < dimCol_s; j++) {
           if (mask_s[i][j] == 1) {
              nobs += 1;
           }
        }
      }
      // save pathFlag to MetaData
      std::vector<std::string> intMetadataNames = {"QCPath"};
      std::vector<std::string> floatMetadataNames = {"ObsValue_raw_4"};


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
      obsforge::preproc::iodavars::IodaVars iodaVars(nobs, 1, floatMetadataNames, intMetadataNames);
      iodaVars.referenceDate_ = "seconds since 1970-01-01T00:00:00Z";

      oops::Log::info() << " eigen... row and column:" << obsvalue_s.size() << " "
                        << obsvalue_s[0].size() << std::endl;
      // Store into eigen arrays
      for (int k = 0; k < nchan; k++) {
          iodaVars.channelValues_(k) = channelNumber[k];
          int loc(0);
          for (int i = 0; i < dimRow_s; i++) {
              for (int j = 0; j < dimCol_s; j++) {
                 if (mask_s[i][j] == 1) {                             // mask apply to all channels
                    iodaVars.longitude_(loc) = lon2d_s[i][j];
                    iodaVars.latitude_(loc) = lat2d_s[i][j];
                    iodaVars.datetime_(loc) = secondsSinceReference;
		    iodaVars.intMetadata_.row(loc) << pathflag_s[i][j];
		    iodaVars.floatMetadata_.row(loc) << obsvalue_raw_s[i][j];
                    // VIIRS AOD use only one channel (4)
                    iodaVars.obsVal_(nchan*loc+k) = obsvalue_s[i][j];
                    if ( fullConfig_.has("binning") ) {
                       iodaVars.preQc_(nchan*loc+k)     = 0;
                    } else {
                       iodaVars.preQc_(nchan*loc+k)     = preqc[i][j];
                    }
                    iodaVars.obsError_(nchan*loc+k) = obserror_s[i][j];
                    loc += 1;
                 }
              }
          }
          oops::Log::info() << " total location "  << loc << std::endl;
      }

      return iodaVars;
    };
  };  // class Viirsaod2Ioda
}  // namespace obsforge
