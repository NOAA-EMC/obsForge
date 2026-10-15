#pragma once

#include "NetCDFToIodaConverter.h"

#include "oops/util/Logger.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <netcdf>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "eckit/config/LocalConfiguration.h"

#include "ioda/Group.h"
#include "ioda/ObsGroup.h"

#include "superob.h"
#include "util.h"
using netCDF::exceptions::NcException;

namespace obsforge {

class Goesaod2Ioda : public NetCDFToIodaConverter {
public:
explicit Goesaod2Ioda(const eckit::Configuration & fullConfig,
const eckit::mpi::Comm & comm)
: NetCDFToIodaConverter(fullConfig, comm) {
variable_ = "aerosolOpticalDepth";
}

obsforge::preproc::iodavars::IodaVars providerToIodaVars(
    const std::string fileName) override {
  oops::Log::info() << "Processing GOES ABI AOD file" << std::endl;
  oops::Log::info() << "Reading... " << fileName << std::endl;

  // --------------------------------------------------------------------
  // Open NetCDF file
  // --------------------------------------------------------------------
  netCDF::NcFile ncFile(fileName, netCDF::NcFile::read);

  // --------------------------------------------------------------------
  // Dimensions
  // --------------------------------------------------------------------
  const int dimRow = static_cast<int>(ncFile.getDim("y").getSize());
  const int dimCol = static_cast<int>(ncFile.getDim("x").getSize());

  oops::Log::info() << "GOES dimensions: rows = "
                    << dimRow << ", columns = " << dimCol << std::endl;

  // --------------------------------------------------------------------
  // Read raw variables.
  //
  // AOD is ushort:
  //   physical AOD = raw * scale_factor + add_offset
  //
  // DQF is ubyte:
  //   0 = high quality
  //   1 = medium quality
  //   2 = low quality
  //   3 = no retrieval
  //
  // x and y are short fixed-grid coordinates:
  //   x = raw_x * 5.6e-05 - 0.151844
  //   y = raw_y * (-5.6e-05) + 0.151844
  // --------------------------------------------------------------------

  std::vector<unsigned short> aodRaw(
      static_cast<std::size_t>(dimRow) * dimCol);

  std::vector<unsigned char> dqf(
      static_cast<std::size_t>(dimRow) * dimCol);

  std::vector<short> xRaw(dimCol);
  std::vector<short> yRaw(dimRow);

  ncFile.getVar("AOD").getVar(aodRaw.data());
  ncFile.getVar("DQF").getVar(dqf.data());
  ncFile.getVar("x").getVar(xRaw.data());
  ncFile.getVar("y").getVar(yRaw.data());

  // --------------------------------------------------------------------
  // Read time.
  //
  // t is seconds since 2000-01-01 12:00:00.
  // IODA output uses seconds since 1970-01-01 00:00:00.
  //
  // Difference between the two epochs:
  // 2000-01-01 12:00:00 - 1970-01-01 00:00:00
  // = 946728000 seconds.
  // --------------------------------------------------------------------

  double goesTime = 0.0;
  ncFile.getVar("t").getVar(&goesTime);

  constexpr double goesEpochToUnix = 946728000.0;
  const double secondsSinceReference =
      goesTime + goesEpochToUnix;

  oops::Log::info() << "GOES time (seconds since J2000 epoch) = "
                    << goesTime << std::endl;
  oops::Log::info() << "IODA time (seconds since Unix epoch) = "
                    << secondsSinceReference << std::endl;

  // --------------------------------------------------------------------
  // Read GOES ABI projection metadata from the input file.
  // --------------------------------------------------------------------
  double longitudeOriginDeg = 0.0;
  double perspectivePointHeight = 0.0;
  double semiMajorAxis = 0.0;
  double semiMinorAxis = 0.0;

  const auto projection = ncFile.getVar("goes_imager_projection");
  if (projection.isNull()) {
    throw std::runtime_error(
        "Input file is missing goes_imager_projection metadata.");
  }

  projection.getAtt("longitude_of_projection_origin")
      .getValues(&longitudeOriginDeg);
  projection.getAtt("perspective_point_height")
      .getValues(&perspectivePointHeight);
  projection.getAtt("semi_major_axis")
      .getValues(&semiMajorAxis);
  projection.getAtt("semi_minor_axis")
      .getValues(&semiMinorAxis);

  // The shared ABI projection utility expects distance from Earth's center.
  const double satelliteDistance =
      perspectivePointHeight + semiMajorAxis;

  // --------------------------------------------------------------------
  // Unpack fixed-grid x/y coordinates.
  // --------------------------------------------------------------------
  std::vector<double> x(dimCol);
  std::vector<double> y(dimRow);

  constexpr double xScale = 5.6e-05;
  constexpr double yScale = -5.6e-05;
  constexpr double xOffset = -0.151844;
  constexpr double yOffset = 0.151844;

  for (int j = 0; j < dimCol; ++j) {
    x[j] = static_cast<double>(xRaw[j]) * xScale + xOffset;
  }

  for (int i = 0; i < dimRow; ++i) {
    y[i] = static_cast<double>(yRaw[i]) * yScale + yOffset;
  }

  // Expand the fixed-grid coordinates into 2-D grids for the shared
  // GOES ABI inverse-projection utility.
  std::vector<std::vector<double>> xGrid(
      dimRow, std::vector<double>(dimCol));
  std::vector<std::vector<double>> yGrid(
      dimRow, std::vector<double>(dimCol));

  for (int i = 0; i < dimRow; ++i) {
    for (int j = 0; j < dimCol; ++j) {
      xGrid[i][j] = x[j];
      yGrid[i][j] = y[i];
    }
  }

  std::vector<std::vector<double>> latitudeGrid;
  std::vector<std::vector<double>> longitudeGrid;

  obsforge::preproc::utils::abiToGeoLoc(
      xGrid, yGrid,
      longitudeOriginDeg,
      satelliteDistance,
      semiMajorAxis,
      semiMinorAxis,
      &latitudeGrid,
      &longitudeGrid);

  // --------------------------------------------------------------------
  // Allocate 2-D arrays using the same convention as the VIIRS
  // converter.
  // --------------------------------------------------------------------
  std::vector<std::vector<int>> mask(
      dimRow, std::vector<int>(dimCol, 0));

  std::vector<std::vector<float>> obsvalue(
      dimRow, std::vector<float>(dimCol, 0.0f));

  std::vector<std::vector<float>> obserror(
      dimRow, std::vector<float>(dimCol, 0.0f));

  std::vector<std::vector<int>> preqc(
      dimRow, std::vector<int>(dimCol, 0));

  std::vector<std::vector<float>> lat(
      dimRow, std::vector<float>(dimCol, 0.0f));

  std::vector<std::vector<float>> lon(
      dimRow, std::vector<float>(dimCol, 0.0f));

  // --------------------------------------------------------------------
  // Thinning and QC
  // --------------------------------------------------------------------
  float thinThreshold;
  fullConfig_.get("thinning.threshold", thinThreshold);

  int preQcValue;
  fullConfig_.get("preqc", preQcValue);

  oops::Log::info() << "thinthreshold "
                    << thinThreshold << std::endl;

  oops::Log::info() << "preqc "
                    << preQcValue << std::endl;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dis(0.0, 1.0);

  constexpr unsigned short aodFillValue = 65535;
  constexpr float aodScale = 7.706e-05f;
  constexpr float aodOffset = -0.05f;

  int nHighQuality = 0;
  int nValid = 0;

  for (int i = 0; i < dimRow; ++i) {
    for (int j = 0; j < dimCol; ++j) {
      const std::size_t index =
          static_cast<std::size_t>(i) * dimCol + j;

      const unsigned short rawAod = aodRaw[index];
      const int qc = static_cast<int>(dqf[index]);

      // Missing AOD.
      if (rawAod == aodFillValue) {
        continue;
      }

      // Invalid DQF.
      if (qc > 3) {
        continue;
      }

      // Apply configured QC threshold.
      if (qc > preQcValue) {
        continue;
      }

      ++nHighQuality;

      // Random thinning.
      const float isThin =
          static_cast<float>(dis(gen));

      if (isThin > thinThreshold) {
        const float aod =
            static_cast<float>(rawAod) * aodScale + aodOffset;

        // Physical valid range from the GOES file.
        if (aod < 0.0f) {
          continue;
        }

        if (aod > 5.0f) {
          continue;
        }

        const double latitude = latitudeGrid[i][j];
        const double longitude = longitudeGrid[i][j];

        // The projection utility returns NaN when the viewing ray
        // does not intersect Earth. Reject those pixels before masking.
        if (!std::isfinite(latitude) || !std::isfinite(longitude)) {
          continue;
        }

        // Reject coordinates outside physical bounds.
        if (latitude < -90.0 || latitude > 90.0 ||
            longitude < -180.0 || longitude > 180.0) {
          continue;
        }

        obsvalue[i][j] = aod;
        lat[i][j] = static_cast<float>(latitude);
        lon[i][j] = static_cast<float>(longitude);

        // GOES ABI AOD observation error.
        //
        // This is the same simple AOD-dependent form used in the
        // initial GOES converter.  It can be replaced with the
        // operational GOES error model if one is available.
        obserror[i][j] =
            0.05f + 0.20f * aod;

        preqc[i][j] = qc;
        mask[i][j] = 1;

        ++nValid;
      }
    }
  }

  oops::Log::info()
      << "GOES valid high-quality pixels before thinning = "
      << nHighQuality << std::endl;

  oops::Log::info()
      << "GOES observations retained after thinning = "
      << nValid << std::endl;

  // --------------------------------------------------------------------
  // Optional superobbing/binning.
  // Use the same implementation as the VIIRS converter.
  // --------------------------------------------------------------------
  std::vector<std::vector<float>> obsvalue_s;
  std::vector<std::vector<float>> lon2d_s;
  std::vector<std::vector<float>> lat2d_s;
  std::vector<std::vector<float>> obserror_s;
  std::vector<std::vector<int>> mask_s;

  if (fullConfig_.has("binning")) {
    float minLon =
        std::numeric_limits<float>::max();

    float maxLon =
        std::numeric_limits<float>::lowest();

    for (const auto & row : lon) {
      minLon = std::min(
          minLon, *std::min_element(row.begin(), row.end()));

      maxLon = std::max(
          maxLon, *std::max_element(row.begin(), row.end()));
    }

    if (maxLon - minLon > 180.0f) {
      for (auto & row : lon) {
        for (float & lonValue : row) {
          lonValue = std::fmod(
              lonValue + 360.0f, 360.0f);
        }
      }
    }

    lon2d_s =
        obsforge::superobutils::subsample2D(
            lon, mask, fullConfig_);

    for (auto & row : lon2d_s) {
      for (float & lonValue : row) {
        lonValue = std::fmod(
            lonValue + 360.0f, 360.0f);
      }
    }

    lat2d_s =
        obsforge::superobutils::subsample2D(
            lat, mask, fullConfig_);

    mask_s =
        obsforge::superobutils::subsample2D(
            mask, mask, fullConfig_);

    if (fullConfig_.has("binning.cressman radius")) {
      bool useCressman = true;

      obsvalue_s =
          obsforge::superobutils::subsample2D(
              obsvalue, mask, fullConfig_,
              useCressman, lat, lon,
              lat2d_s, lon2d_s);

      obserror_s =
          obsforge::superobutils::subsample2D(
              obserror, mask, fullConfig_,
              useCressman, lat, lon,
              lat2d_s, lon2d_s);
    } else {
      obsvalue_s =
          obsforge::superobutils::subsample2D(
              obsvalue, mask, fullConfig_);

      obserror_s =
          obsforge::superobutils::subsample2D(
              obserror, mask, fullConfig_);
    }
  } else {
    obsvalue_s = obsvalue;
    lon2d_s = lon;
    lat2d_s = lat;
    obserror_s = obserror;
    mask_s = mask;
  }

  // Cressman interpolation can return -9999 when no valid source
  // observations fall within the radius. Do not export those bins.
  if (fullConfig_.has("binning")) {
    for (std::size_t i = 0; i < mask_s.size(); ++i) {
      for (std::size_t j = 0; j < mask_s[i].size(); ++j) {
        if (obsvalue_s[i][j] == -9999.0f ||
            obserror_s[i][j] == -9999.0f) {
          mask_s[i][j] = 0;
        }
      }
    }
  }

  // --------------------------------------------------------------------
  // Count observations.
  // --------------------------------------------------------------------
  const int dimRow_s =
      static_cast<int>(obsvalue_s.size());

  const int dimCol_s =
      static_cast<int>(obsvalue_s[0].size());

  int nobs = 0;

  for (int i = 0; i < dimRow_s; ++i) {
    for (int j = 0; j < dimCol_s; ++j) {
      if (mask_s[i][j] == 1) {
        ++nobs;
      }
    }
  }

  oops::Log::info()
      << "Final number of GOES observations = "
      << nobs << std::endl;

  // --------------------------------------------------------------------
  // Read channel number from YAML.
  // GOES ABI AOD is a 550 nm product.  The YAML currently specifies
  // channel 4.
  // --------------------------------------------------------------------
  std::string channels;
  fullConfig_.get("channel", channels);

  std::istringstream ss(channels);
  std::vector<int> channelNumber;

  std::string substr;

  while (std::getline(ss, substr, ',')) {
    const int intValue = std::stoi(substr);
    channelNumber.push_back(intValue);
  }

  if (channelNumber.size() != 1) {
    throw std::runtime_error(
        "GOESAOD requires exactly one channel in the configuration.");
  }

  oops::Log::info()
      << "channels " << channelNumber << std::endl;

  const int nchan =
      static_cast<int>(channelNumber.size());

  oops::Log::info()
      << "number of channels " << nchan << std::endl;

  // --------------------------------------------------------------------
  // Create IodaVars object.
  // --------------------------------------------------------------------
  obsforge::preproc::iodavars::IodaVars iodaVars(
      nobs, 1, {}, {});

  iodaVars.referenceDate_ =
      "seconds since 1970-01-01T00:00:00Z";

  // --------------------------------------------------------------------
  // Store observations in Eigen arrays.
  // --------------------------------------------------------------------
  for (int k = 0; k < nchan; ++k) {
    iodaVars.channelValues_(k) =
        channelNumber[k];

    int loc = 0;

    for (int i = 0; i < dimRow_s; ++i) {
      for (int j = 0; j < dimCol_s; ++j) {
        if (mask_s[i][j] == 1) {
          iodaVars.longitude_(loc) =
              lon2d_s[i][j];

          iodaVars.latitude_(loc) =
              lat2d_s[i][j];

          iodaVars.datetime_(loc) =
              secondsSinceReference;

          iodaVars.obsVal_(nchan * loc + k) =
              obsvalue_s[i][j];

          if (fullConfig_.has("binning")) {
            iodaVars.preQc_(nchan * loc + k) = 0;
          } else {
            iodaVars.preQc_(nchan * loc + k) =
                preqc[i][j];
          }

          iodaVars.obsError_(nchan * loc + k) =
              obserror_s[i][j];

          ++loc;
        }
      }
    }

    oops::Log::info()
        << "total location " << loc << std::endl;
  }

  return iodaVars;
}

};  // class Goesaod2Ioda

}  // namespace obsforge
