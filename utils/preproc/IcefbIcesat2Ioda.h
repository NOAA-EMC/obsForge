#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <netcdf>    // NOLINT (using C API)
#include <sstream>
#include <string>
#include <vector>

#include "eckit/config/LocalConfiguration.h"

#include <Eigen/Dense>    // NOLINT

#include "ioda/core/IodaUtils.h"
#include "ioda/Group.h"
#include "ioda/ObsGroup.h"

#include "oops/util/dateFunctions.h"

#include "NetCDFToIodaConverter.h"

namespace obsforge {

  class IcefbIcesat2Ioda : public NetCDFToIodaConverter {
   public:
    explicit IcefbIcesat2Ioda(const eckit::Configuration & fullConfig, const eckit::mpi::Comm & comm)
      : NetCDFToIodaConverter(fullConfig, comm) {
      variable_ = "seaIceFreeboard";

      if (fullConfig_.has("thin stride")) {
        fullConfig_.get("thin stride", thinStride_);
      }
      ASSERT(thinStride_ >= 1);

      if (fullConfig_.has("min freeboard")) {
        fullConfig_.get("min freeboard", minFreeboard_);
        useMinFreeboard_ = true;
      }
      if (fullConfig_.has("max freeboard")) {
        fullConfig_.get("max freeboard", maxFreeboard_);
        useMaxFreeboard_ = true;
      }
      if (fullConfig_.has("max quality flag")) {
        fullConfig_.get("max quality flag", maxQualityFlag_);
      }
      if (fullConfig_.has("keep invalid quality flag")) {
        fullConfig_.get("keep invalid quality flag", keepInvalidQualityFlag_);
      }
      if (fullConfig_.has("default obs error")) {
        fullConfig_.get("default obs error", defaultObsError_);
      }

      northOutputFilename_ = outputFilenameWithPole(outputFilename_, "north");
      southOutputFilename_ = outputFilenameWithPole(outputFilename_, "south");
      if (fullConfig_.has("north output file")) {
        fullConfig_.get("north output file", northOutputFilename_);
      }
      if (fullConfig_.has("south output file")) {
        fullConfig_.get("south output file", southOutputFilename_);
      }
    }

    void writeToIoda() {
      if (inputFilenames_.empty()) {
        writeEmptyIodaFile(northOutputFilename_);
        writeEmptyIodaFile(southOutputFilename_);
        return;
      }

      obsforge::preproc::iodavars::IodaVars iodaVarsAll = collectIodaVars();
      if (comm_.rank() == 0) {
        obsforge::preproc::iodavars::IodaVars northVars = iodaVarsAll;
        Eigen::Array<bool, Eigen::Dynamic, 1> northMask = northVars.latitude_ >= 0.0f;
        northVars.trim(northMask);
        northVars.strGlobalAttr_["pole"] = "north";
        writeIodaFile(northOutputFilename_, &northVars);

        obsforge::preproc::iodavars::IodaVars southVars = iodaVarsAll;
        Eigen::Array<bool, Eigen::Dynamic, 1> southMask = southVars.latitude_ < 0.0f;
        southVars.trim(southMask);
        southVars.strGlobalAttr_["pole"] = "south";
        writeIodaFile(southOutputFilename_, &southVars);
      }
    }

    // Read ATL10 granule and populate IodaVars.
    obsforge::preproc::iodavars::IodaVars providerToIodaVars(const std::string fileName) final {
      oops::Log::info() << "Processing files provided by ICESat-2 ATL10" << std::endl;
      oops::Log::info() << "Reading... " << fileName << std::endl;

      netCDF::NcFile ncFile(fileName, netCDF::NcFile::read);

      std::vector<float> latValues;
      std::vector<float> lonValues;
      std::vector<int64_t> datetimeValues;
      std::vector<float> freeboardValues;
      std::vector<float> errorValues;
      std::vector<int> preQcValues;
      std::vector<float> confidenceValues;
      std::vector<int> beamValues;

      const int64_t atl10EpochOffset = atl10EpochOffsetSeconds();
      const int64_t windowBeginSeconds = epochOffsetSeconds(windowBegin_);
      const int64_t windowEndSeconds = epochOffsetSeconds(windowEnd_);

      for (const auto & beam : beams_) {
        netCDF::NcGroup beamGroup = ncFile.getGroup(beam);
        if (beamGroup.isNull()) {
          continue;
        }

        netCDF::NcGroup segment = beamGroup.getGroup("freeboard_segment");
        if (segment.isNull()) {
          continue;
        }

        std::vector<double> lat = readSampled1D<double>(segment, "latitude");
        std::vector<double> lon = readSampled1D<double>(segment, "longitude");
        std::vector<double> deltaTime = readSampled1D<double>(segment, "delta_time");
        std::vector<float> freeboard = readSampled1D<float>(segment, "beam_fb_height");
        std::vector<float> uncertainty = readSampled1D<float>(segment, "beam_fb_unc");
        std::vector<int> qualityFlag = readSampled1D<int>(segment, "beam_fb_quality_flag");
        std::vector<float> confidence = readSampled1D<float>(segment, "beam_fb_confidence");

        const size_t nobs = minimumSize({lat.size(), lon.size(), deltaTime.size(),
                                         freeboard.size(), uncertainty.size(),
                                         qualityFlag.size(), confidence.size()});
        const int beamId = beamIdentifier(beam);

        for (size_t i = 0; i < nobs; i++) {
          if (!validLatitude(lat[i]) || !validLongitude(lon[i]) ||
              !validDouble(deltaTime[i]) || !validFloat(freeboard[i]) ||
              !validQualityFlag(qualityFlag[i])) {
            continue;
          }
          const int64_t datetime = atl10EpochOffset + static_cast<int64_t>(std::llround(deltaTime[i]));
          if (datetime < windowBeginSeconds || datetime > windowEndSeconds) {
            continue;
          }
          if (useMinFreeboard_ && freeboard[i] < minFreeboard_) {
            continue;
          }
          if (useMaxFreeboard_ && freeboard[i] > maxFreeboard_) {
            continue;
          }

          latValues.push_back(static_cast<float>(lat[i]));
          lonValues.push_back(static_cast<float>(lon[i]));
          datetimeValues.push_back(datetime);
          freeboardValues.push_back(freeboard[i]);
          errorValues.push_back(validError(uncertainty[i]) ? uncertainty[i] : defaultObsError_);
          preQcValues.push_back(qualityFlag[i]);
          confidenceValues.push_back(validFloat(confidence[i]) ? confidence[i] : missingFloat_);
          beamValues.push_back(beamId);
        }
      }

      std::vector<std::string> floatMetadataNames = {"freeboardConfidence"};
      std::vector<std::string> intMetadataNames = {"beam", "oceanBasin"};
      obsforge::preproc::iodavars::IodaVars iodaVars(latValues.size(), 1,
                                                     floatMetadataNames,
                                                     intMetadataNames);
      iodaVars.referenceDate_ = "seconds since 1970-01-01T00:00:00Z";
      iodaVars.strGlobalAttr_["platform"] = "ICESat-2";
      iodaVars.strGlobalAttr_["instrument"] = "ATLAS";
      iodaVars.strGlobalAttr_["source"] = "ATL10 L3A Sea Ice Freeboard";
      iodaVars.strGlobalAttr_["beam_index"] = beamIndexDescription();

      for (int i = 0; i < iodaVars.location_; i++) {
        iodaVars.latitude_(i) = latValues[i];
        iodaVars.longitude_(i) = lonValues[i];
        iodaVars.datetime_(i) = datetimeValues[i];
        iodaVars.obsVal_(i) = freeboardValues[i];
        iodaVars.obsError_(i) = errorValues[i];
        iodaVars.preQc_(i) = preQcValues[i];
        iodaVars.floatMetadata_.row(i) << confidenceValues[i];
        iodaVars.intMetadata_.row(i) << beamValues[i], -999;
      }

      oops::Log::info() << "ICESat-2 ATL10 valid observations: "
                        << iodaVars.location_ << std::endl;
      return iodaVars;
    }

   private:
    template <typename T>
    std::vector<T> readSampled1D(const netCDF::NcGroup & group, const std::string & variableName) const {
      netCDF::NcVar variable = group.getVar(variableName);
      if (variable.isNull()) {
        oops::Log::warning() << "Missing ATL10 variable: " << variableName << std::endl;
        return {};
      }

      const std::vector<netCDF::NcDim> dims = variable.getDims();
      if (dims.size() != 1) {
        oops::Log::warning() << "ATL10 variable has unexpected rank, skipping: "
                              << variableName << std::endl;
        return {};
      }
      const size_t dimSize = dims[0].getSize();
      const size_t sampledSize = (dimSize + thinStride_ - 1) / thinStride_;
      std::vector<T> values(sampledSize);

      if (sampledSize == 0) {
        return values;
      }

      const std::vector<size_t> start = {0};
      const std::vector<size_t> count = {sampledSize};
      const std::vector<std::ptrdiff_t> stride = {thinStride_};
      variable.getVar(start, count, stride, values.data());
      return values;
    }

    int64_t epochOffsetSeconds(const util::DateTime & dateTime) const {
      util::DateTime epochDtime("1970-01-01T00:00:00Z");
      return ioda::convertDtimeToTimeOffsets(epochDtime, {dateTime})[0];
    }

    int64_t atl10EpochOffsetSeconds() const {
      util::DateTime atl10Epoch("2018-01-01T00:00:00Z");
      return epochOffsetSeconds(atl10Epoch);
    }

    static bool validFloat(const float value) {
      return std::isfinite(value) && std::abs(value) < 1.0e20f;
    }

    static bool validDouble(const double value) {
      return std::isfinite(value) && std::abs(value) < 1.0e20;
    }

    static bool validLatitude(const double value) {
      return validDouble(value) && value >= -90.0 && value <= 90.0;
    }

    static bool validLongitude(const double value) {
      return validDouble(value) && value >= -180.0 && value <= 180.0;
    }

    bool validQualityFlag(const int value) const {
      if (keepInvalidQualityFlag_) {
        return true;
      }
      return value >= 1 && value <= maxQualityFlag_;
    }

    bool validError(const float value) const {
      return validFloat(value) && value >= 0.0f;
    }

    static size_t minimumSize(const std::vector<size_t> & sizes) {
      size_t result = std::numeric_limits<size_t>::max();
      for (const size_t size : sizes) {
        result = std::min(result, size);
      }
      return result == std::numeric_limits<size_t>::max() ? 0 : result;
    }

    static int beamIdentifier(const std::string & beam) {
      if (beam == "gt1l") return 11;
      if (beam == "gt1r") return 12;
      if (beam == "gt2l") return 21;
      if (beam == "gt2r") return 22;
      if (beam == "gt3l") return 31;
      if (beam == "gt3r") return 32;
      return -1;
    }

    static std::string beamIndexDescription() {
      std::stringstream description;
      description << "gt1l=11 gt1r=12 gt2l=21 gt2r=22 gt3l=31 gt3r=32";
      return description.str();
    }

    static std::string outputFilenameWithPole(const std::string & outputFilename,
                                              const std::string & pole) {
      const std::string iodaExtension = ".ioda";
      size_t pos = outputFilename.rfind(iodaExtension);
      if (pos != std::string::npos) {
        return outputFilename.substr(0, pos) + "_" + pole + outputFilename.substr(pos);
      }

      const std::string ncExtension = ".nc";
      pos = outputFilename.rfind(ncExtension);
      if (pos != std::string::npos) {
        return outputFilename.substr(0, pos) + "_" + pole + outputFilename.substr(pos);
      }

      return outputFilename + "_" + pole;
    }

    static constexpr float missingFloat_ = std::numeric_limits<float>::max();
    static constexpr std::array<const char *, 6> beams_ = {
      "gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"
    };

    int thinStride_ = 1;
    bool useMinFreeboard_ = false;
    bool useMaxFreeboard_ = false;
    double minFreeboard_ = 0.0;
    double maxFreeboard_ = 0.0;
    int maxQualityFlag_ = 5;
    bool keepInvalidQualityFlag_ = false;
    float defaultObsError_ = 0.5f;
    std::string northOutputFilename_;
    std::string southOutputFilename_;
  };  // class IcefbIcesat2Ioda
}  // namespace obsforge
