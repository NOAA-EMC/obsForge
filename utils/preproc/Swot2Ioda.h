#pragma once

#include <cmath>
#include <iostream>
#include <map>
#include <netcdf>    // NOLINT (using C API)
#include <regex>
#include <string>
#include <vector>

#include "eckit/config/LocalConfiguration.h"

#include <Eigen/Dense>    // NOLINT

#include "ioda/Group.h"
#include "ioda/ObsGroup.h"

#include "NetCDFToIodaConverter.h"
#include "superob.h"  // NOLINT

namespace obsforge {

  // Converter for the SWOT KaRIn L2 LR SSH (Expert) swath product.
  // Produces the same obs space as Rads2Ioda (absoluteDynamicTopography), but the
  // observations come from a 2D swath (num_lines x num_pixels) instead of a nadir track.
  class Swot2Ioda : public NetCDFToIodaConverter {
   public:
    explicit Swot2Ioda(const eckit::Configuration & fullConfig, const eckit::mpi::Comm & comm)
      : NetCDFToIodaConverter(fullConfig, comm) {
      variable_ = "absoluteDynamicTopography";
    }

    // Read netcdf file and populate iodaVars
    obsforge::preproc::iodavars::IodaVars providerToIodaVars(const std::string fileName) final {
      oops::Log::info() << "Processing files provided by SWOT (KaRIn)" << std::endl;

      //  Abort the case where the 'error ratio' key is not found
      ASSERT(fullConfig_.has("error ratio"));

      // Get the obs. error ratio from the configuration (meters per day)
      float errRatio;
      fullConfig_.get("error ratio", errRatio);
      // Convert errRatio from meters per day to meters per second
      errRatio /= 86400.0;

      // SSHA field: ssha_karin_2 (model wet troposphere, default) or ssha_karin (radiometer)
      const std::string sshaName = fullConfig_.getString("ssha variable", "ssha_karin_2");

      // Open the NetCDF file in read-only mode
      netCDF::NcFile ncFile(fileName, netCDF::NcFile::read);
      oops::Log::info() << "Reading... " << fileName << std::endl;

      // Swath dimensions
      const int nLines = ncFile.getDim("num_lines").getSize();
      const int nPixels = ncFile.getDim("num_pixels").getSize();
      const size_t nSwath = static_cast<size_t>(nLines) * nPixels;

      // Read and unpack the swath fields, valid is false wherever a field is missing
      std::vector<bool> valid(nSwath, true);
      std::vector<double> lat = readSwathVar<int32_t>(ncFile, "latitude", &valid);
      std::vector<double> lon = readSwathVar<int32_t>(ncFile, "longitude", &valid);
      std::vector<double> ssha = readSwathVar<int32_t>(ncFile, sshaName, &valid);
      std::vector<double> xover = readSwathVar<int32_t>(ncFile, "height_cor_xover", &valid);
      std::vector<double> mdt = readSwathVar<int16_t>(ncFile, "mean_dynamic_topography", &valid);
      std::vector<double> xtrack = readSwathVar<float>(ncFile, "cross_track_distance", &valid);

      // Quality and surface flags, 0 is good/open ocean/no ice/no rain for all of them.
      // Fill values are non-zero and are rejected as well.
      std::vector<uint32_t> sshaQual(nSwath);
      ncFile.getVar(sshaName + "_qual").getVar(sshaQual.data());
      std::vector<uint8_t> xoverQual(nSwath);
      ncFile.getVar("height_cor_xover_qual").getVar(xoverQual.data());
      std::vector<uint8_t> surfaceFlag(nSwath);
      ncFile.getVar("ancillary_surface_classification_flag").getVar(surfaceFlag.data());
      std::vector<uint8_t> iceFlag(nSwath);
      ncFile.getVar("dynamic_ice_flag").getVar(iceFlag.data());
      std::vector<uint8_t> rainFlag(nSwath);
      ncFile.getVar("rain_flag").getVar(rainFlag.data());

      // Time is only a function of the along track line
      std::vector<double> time(nLines);
      ncFile.getVar("time").getVar(time.data());
      double timeFillValue;
      ncFile.getVar("time").getAtt("_FillValue").getValues(&timeFillValue);

      // Reformat the reference time: "seconds since 2000-01-01 00:00:00.0"
      std::string timeUnits;
      ncFile.getVar("time").getAtt("units").getValues(timeUnits);
      std::regex dateRegex(R"(seconds since (\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}))");
      std::smatch match;
      ASSERT(std::regex_search(timeUnits, match, dateRegex));
      std::string refDate = "seconds since " + match.str(1) + "T" + match.str(2) + "Z";

      // Cycle and pass numbers
      int cycle;
      ncFile.getAtt("cycle_number").getValues(&cycle);
      int pass;
      ncFile.getAtt("pass_number").getValues(&pass);

      // ADT = SSHA + crossover calibration + MDT, on a 2D swath
      std::vector<std::vector<int>> mask(nLines, std::vector<int>(nPixels));
      std::vector<std::vector<float>> adt2d(nLines, std::vector<float>(nPixels));
      std::vector<std::vector<float>> mdt2d(nLines, std::vector<float>(nPixels));
      std::vector<std::vector<float>> lat2d(nLines, std::vector<float>(nPixels));
      std::vector<std::vector<float>> lon2d(nLines, std::vector<float>(nPixels));
      std::vector<std::vector<float>> xtrack2d(nLines, std::vector<float>(nPixels));
      std::vector<std::vector<double>> seconds2d(nLines, std::vector<double>(nPixels));
      size_t index = 0;
      for (int i = 0; i < nLines; i++) {
        for (int j = 0; j < nPixels; j++) {
          adt2d[i][j] = static_cast<float>(ssha[index] + xover[index] + mdt[index]);
          mdt2d[i][j] = static_cast<float>(mdt[index]);
          lat2d[i][j] = static_cast<float>(lat[index]);
          // longitude from [0, 360) to [-180, 180)
          lon2d[i][j] = static_cast<float>(lon[index] >= 180.0 ? lon[index] - 360.0 : lon[index]);
          xtrack2d[i][j] = static_cast<float>(xtrack[index]);
          seconds2d[i][j] = time[i];

          // Basic QC
          mask[i][j] = (valid[index] && time[i] != timeFillValue &&
                        sshaQual[index] == 0 && xoverQual[index] == 0 &&
                        surfaceFlag[index] == 0 && iceFlag[index] == 0 && rainFlag[index] == 0 &&
                        adt2d[i][j] > -4.0 && adt2d[i][j] < 4.0) ? 1 : 0;
          index++;
        }
      }

      // Superobing over (stride x stride) boxes of swath pixels
      if ( fullConfig_.has("binning") ) {
        // Average the longitude as a unit vector to avoid issues at the dateline
        std::vector<std::vector<float>> coslon2d(nLines, std::vector<float>(nPixels));
        std::vector<std::vector<float>> sinlon2d(nLines, std::vector<float>(nPixels));
        for (int i = 0; i < nLines; i++) {
          for (int j = 0; j < nPixels; j++) {
            coslon2d[i][j] = std::cos(lon2d[i][j] * M_PI / 180.0);
            sinlon2d[i][j] = std::sin(lon2d[i][j] * M_PI / 180.0);
          }
        }
        coslon2d = binSwath(coslon2d, mask);
        sinlon2d = binSwath(sinlon2d, mask);
        adt2d = binSwath(adt2d, mask);
        mdt2d = binSwath(mdt2d, mask);
        lat2d = binSwath(lat2d, mask);
        xtrack2d = binSwath(xtrack2d, mask);
        seconds2d = binSwath(seconds2d, mask);

        // Bins with less than "min number of obs" are set to -9999
        mask.assign(adt2d.size(), std::vector<int>(adt2d[0].size()));
        lon2d.assign(adt2d.size(), std::vector<float>(adt2d[0].size()));
        for (int i = 0; i < adt2d.size(); i++) {
          for (int j = 0; j < adt2d[0].size(); j++) {
            mask[i][j] = (adt2d[i][j] != -9999.0) ? 1 : 0;
            lon2d[i][j] = std::atan2(sinlon2d[i][j], coslon2d[i][j]) * 180.0 / M_PI;
          }
        }
      }

      // Number of valid obs
      int nobs = 0;
      for (const auto & row : mask) {
        for (const int m : row) nobs += m;
      }

      // Set the int metadata names
      std::vector<std::string> intMetadataNames = {"cycle", "pass", "mission", "oceanBasin"};

      // Set the float metadata name
      std::vector<std::string> floatMetadataNames = {"mdt", "crossTrackDistance"};

      // Create instance of iodaVars object
      obsforge::preproc::iodavars::IodaVars iodaVars(nobs, 1, floatMetadataNames, intMetadataNames);
      iodaVars.referenceDate_ = refDate;

      // Mission index, continues the altimeter list of Rads2Ioda
      const int missionIndex = 8;
      iodaVars.strGlobalAttr_["mission_index"] = "SWOT = " + std::to_string(missionIndex);
      iodaVars.strGlobalAttr_["ssha_variable"] = sshaName;
      std::string references;
      ncFile.getAtt("references").getValues(references);
      iodaVars.strGlobalAttr_["references"] = references;

      // Store into eigen arrays
      int loc = 0;
      for (int i = 0; i < mask.size(); i++) {
        for (int j = 0; j < mask[0].size(); j++) {
          if (mask[i][j] == 0) continue;
          iodaVars.longitude_(loc) = lon2d[i][j];
          iodaVars.latitude_(loc)  = lat2d[i][j];
          iodaVars.datetime_(loc)  = static_cast<int64_t>(std::round(seconds2d[i][j]));
          iodaVars.obsVal_(loc)    = adt2d[i][j];
          iodaVars.obsError_(loc)  = 0.1;  // only within DA window
          iodaVars.preQc_(loc)     = 0;
          // Store optional metadata, set ocean basins to -999 for now
          iodaVars.intMetadata_.row(loc) << cycle, pass, missionIndex, -999;
          iodaVars.floatMetadata_.row(loc) << mdt2d[i][j], xtrack2d[i][j];
          loc++;
        }
      }

      // Extract EpochTime String Format(2000-01-01T00:00:00Z)
      std::string extractedDate = iodaVars.referenceDate_.substr(14);

      // Redating and adjusting Errors
      if (iodaVars.datetime_.size() == 0) {
        oops::Log::info() << "datetime_ is empty" << std::endl;
      } else {
        iodaVars.reDate(windowBegin_, windowEnd_, extractedDate, errRatio);
      }

      return iodaVars;
    };

   private:
    // Superob the left and right sides of the swath separately so that the bins never
    // straddle the nadir gap, the first half of num_pixels is the left side of the swath
    template <typename T>
    std::vector<std::vector<T>> binSwath(const std::vector<std::vector<T>> & field,
                                         const std::vector<std::vector<int>> & mask) const {
      const int nLeft = field[0].size() / 2;
      std::vector<std::vector<T>> fieldLeft, fieldRight;
      std::vector<std::vector<int>> maskLeft, maskRight;
      for (size_t i = 0; i < field.size(); i++) {
        fieldLeft.emplace_back(field[i].begin(), field[i].begin() + nLeft);
        fieldRight.emplace_back(field[i].begin() + nLeft, field[i].end());
        maskLeft.emplace_back(mask[i].begin(), mask[i].begin() + nLeft);
        maskRight.emplace_back(mask[i].begin() + nLeft, mask[i].end());
      }
      std::vector<std::vector<T>> binned =
        obsforge::superobutils::subsample2D(fieldLeft, maskLeft, fullConfig_);
      std::vector<std::vector<T>> binnedRight =
        obsforge::superobutils::subsample2D(fieldRight, maskRight, fullConfig_);
      for (size_t i = 0; i < binned.size(); i++) {
        binned[i].insert(binned[i].end(), binnedRight[i].begin(), binnedRight[i].end());
      }
      return binned;
    }

    // Read a packed (num_lines, num_pixels) variable, apply the scale factor and offset,
    // and set valid to false where the fill value is found
    template <typename T>
    std::vector<double> readSwathVar(const netCDF::NcFile & ncFile, const std::string & varName,
                                     std::vector<bool> * valid) const {
      netCDF::NcVar ncVar = ncFile.getVar(varName);
      ASSERT(ncVar.getDimCount() == 2);
      std::vector<T> raw(valid->size());
      ncVar.getVar(raw.data());

      const auto atts = ncVar.getAtts();
      T fillValue;
      ncVar.getAtt("_FillValue").getValues(&fillValue);
      double scaleFactor = 1.0;
      if (atts.count("scale_factor")) ncVar.getAtt("scale_factor").getValues(&scaleFactor);
      double addOffset = 0.0;
      if (atts.count("add_offset")) ncVar.getAtt("add_offset").getValues(&addOffset);

      std::vector<double> values(raw.size());
      for (size_t i = 0; i < raw.size(); i++) {
        if (raw[i] == fillValue) (*valid)[i] = false;
        values[i] = static_cast<double>(raw[i]) * scaleFactor + addOffset;
      }
      return values;
    }
  };  // class Swot2Ioda
}  // namespace obsforge
