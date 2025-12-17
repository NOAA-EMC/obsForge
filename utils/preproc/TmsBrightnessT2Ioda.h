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
  class TmsBrightnessT2Ioda : public NetCDFToIodaConverter {
   public:
    explicit TmsBrightnessT2Ioda(const eckit::Configuration & fullConfig, const eckit::mpi::Comm & comm)
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
      int dimspot = ncFile.getDim("spots").getSize();
      int dimscan = ncFile.getDim("scans").getSize();
      int dimchan = ncFile.getDim("channels").getSize();
      oops::Log::info() << "spots, scans, channels: " << dimspot << " " << dimscan << " " << dimchan << std::endl;


      // ------------------------------------------------------------------
      // Read variables (flattened storage)
      // ------------------------------------------------------------------
      std::vector<float> lon(dimspot * dimscan);
      std::vector<float> lat(dimspot * dimscan);
      std::vector<float> tb(dimspot * dimscan * dimchan);
      std::vector<int> cqf(dimspot * dimscan * dimchan);

      ncFile.getVar("longitude").getVar(lon.data());
      ncFile.getVar("latitude").getVar(lat.data());
      ncFile.getVar("brightness_temperature").getVar(tb.data());
      ncFile.getVar("combinedQualityFlag").getVar(cqf.data());

      // Read lat, lon and brightness temp (TBR)
      //float lon2d[dimspot][dimscan];
      //ncFile.getVar("longitude").getVar(lon2d);

      //float lat2d[dimspot][dimscan];
      //ncFile.getVar("latitude").getVar(lat2d);

      //float TB[dimspot][dimscan][dimchan];
      //ncFile.getVar("brightness_temperature").getVar(TB);
      // float fillValue;
      // Fill value
      //float tbFillValue;
      //ncFile.getVar("brightness_temperature")
      //      .getAtt("_FillValue")
      //      .getValues(&tbFillValue);

      // Read combinedQualityFlag
      //int8_t comQcF[dimscan][dimchan][dimchan];
      //ncFile.getVar("combinedQualityFlag").getVar(comQcF);

      // ------------------------------------------------------------------
      // Read time components (per scan)
      // ------------------------------------------------------------------
      std::vector<uint16_t> Year(dimscan);
      std::vector<uint8_t> Month(dimscan), Day(dimscan),
                         Hour(dimscan), Minute(dimscan), Second(dimscan);

      ncFile.getVar("Year").getVar(Year.data());
      ncFile.getVar("Month").getVar(Month.data());
      ncFile.getVar("Day").getVar(Day.data());
      ncFile.getVar("Hour").getVar(Hour.data());
      ncFile.getVar("Minute").getVar(Minute.data());
      ncFile.getVar("Second").getVar(Second.data());

      std::vector<double> epochTime(dimscan);
      for (size_t j = 0; j < dimscan; ++j) {
          std::tm t{};
          t.tm_year = Year[j] - 1970;  // year since 1970
          t.tm_mon  = Month[j] - 1;    // months since January (months are 0-based)
          t.tm_mday = Day[j] - 1;          // days since 1st day of the month
          t.tm_hour = Hour[j];
          t.tm_min  = Minute[j];
          t.tm_sec  = (Second[j] == 60 ? 59 : Second[j]);
          t.tm_isdst = 0;

          epochTime[j] = static_cast<double>(timegm(&t));
    }

      // Channel selection
      std::string channelStr;
      fullConfig_.get("channel", channelStr);

      std::vector<int> channels;
      std::stringstream ss(channelStr);
      for (std::string s; std::getline(ss, s, ','); )
         channels.push_back(std::stoi(s));

      oops::Log::info() << "selected channels " << channels << std::endl;
      int nchan = channels.size();
      oops::Log::info() << " number of channels " << nchan << std::endl;

      // Apply scaling/unit change and compute the necessary fields
      //std::vector<std::vector<int>> mask(dimspot, std::vector<float>(dimscan, std::vector<float>(nchan)));
      //std::vector<std::vector<float>> obsvalue(dimspot, std::vector<float>(dimscan, std::vector<float>(nchan)));
      //std::vector<std::vector<float>> obserror(dimspot, std::vector<float>(dimscan, std::vector<float>(nchan)));
      //std::vector<std::vector<int>> preqc(dimspot, std::vector<float>(dimscan, std::vector<float>(nchan)));
      //std::vector<std::vector<float>> lat(dimspot, std::vector<float>(dimscan));
      //std::vector<std::vector<float>> lon(dimspot, std::vector<float>(dimscan));

      // Optional MetaData fields
      // sensor_view_angle
      // sensor_zenith_angle
      //std::vector<std::vector<float>> sensor_view_angle(dimspot, std::vector<float>(dimscan));
      //std::vector<std::vector<float>> sensor_zenith_angle(dimspot, std::vector<float>(dimscan));
      //std::vector<std::vector<float>> sensor_azimuth_angle(dimspot, std::vector<float>(dimscan));

      // Thinning
      float thinThreshold;
      fullConfig_.get("thinning.threshold", thinThreshold);
      //int preQcValue = fullConfig_.get("preqc");
      oops::Log::info() << " thin threshold " << thinThreshold << std::endl;
      //std::random_device rd;
      //std::mt19937 gen(rd());
      std::mt19937 gen(42);    // make thinning results reproducable 
      std::uniform_real_distribution<> dis(0.0, 1.0);

      // ------------------------------------------------------------------
      // Count valid observations
      // ------------------------------------------------------------------
      size_t nlocs = 0;
      for (size_t i = 0; i < dimspot; ++i)
        for (size_t j = 0; j < dimscan; ++j)
          if (dis(gen) > thinThreshold)
            ++nlocs;

      // Create instance of iodaVars object
      obsforge::preproc::iodavars::IodaVars iodaVars(nlocs, {}, {});
      iodaVars.channel_ = nchan;

      // Resize obsVal_ appropriately
      iodaVars.obsVal_.resize(iodaVars.location_ * iodaVars.channel_);
      iodaVars.obsError_.resize(iodaVars.location_ * iodaVars.channel_);
      iodaVars.preQc_.resize(iodaVars.location_ * iodaVars.channel_);

      // Assign values 
      iodaVars.referenceDate_ = "seconds since 1970-01-01T00:00:00Z";
      oops::Log::info() << " eigen... dimspot, dimscan and nchan :" << dimspot << " " << dimscan
                        << " " << nchan << std::endl;
      for (size_t k = 0; k < nchan; k++) {
          iodaVars.channelValues_(k) = channels[k];
      }

      oops::Log::info() << "cqf.size " << cqf.size() << std::endl;
      oops::Log::info() << "tb.size " << tb.size() << std::endl;
      size_t loc = 0;
      for (size_t i = 0; i < dimspot; ++i) {
        for (size_t j = 0; j < dimscan; ++j) {

          if (dis(gen) <= thinThreshold) continue;

          iodaVars.latitude_(loc)  = lat[i*dimscan + j];
          iodaVars.longitude_(loc) = lon[i*dimscan + j];
          iodaVars.datetime_(loc)  = epochTime[j];
          for (size_t k = 0; k < nchan; ++k) {
            size_t ch = channels[k] - 1;
            size_t idx = (i*dimscan + j)*dimchan + ch;

            iodaVars.obsVal_(nchan*loc + k)   = tb[idx];
            iodaVars.obsError_(nchan*loc + k) = 2.0;
            iodaVars.preQc_(nchan*loc + k)    = cqf[idx];
          }
          ++loc;
        }
      }

      oops::Log::info() << " total locations " << loc << std::endl;

      return iodaVars;
    };
  };  // class TmsBrightnessT2Ioda
}  // namespace obsforge

