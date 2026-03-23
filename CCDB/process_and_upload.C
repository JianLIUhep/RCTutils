#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <regex>
#include <set>
#include <bitset>
#include <algorithm>
#include <stdexcept>
#include <ctime>
#include "TFile.h"
#include "TSystem.h"
#include "CCDB/CcdbApi.h"

// Helper function to generate a unique error log filename.
std::string getErrorLogFilename() {
    std::time_t now = std::time(nullptr);
    char buffer[64];
    std::strftime(buffer, sizeof(buffer), "unexpected_flags_%Y%m%d_%H%M%S.log",
                  std::localtime(&now));
    return std::string(buffer);
}

// The function accepts an extra boolean parameter treatNotAvailableAsGood.
void process_and_upload_v2(const char* csvFilePath,
                        const char* passName,
                        const char* periodName,
                        const char* versionNumber,
                        const char* ccdbPath,
                        bool treatNotAvailableAsGood = false) {
    // Load the CCDB dictionary.
    if (gSystem->Load("dict_ccdb.so") < 0) {
        std::cerr << "Error: Failed to load dict_ccdb.so" << std::endl;
        return;
    }

    // Detector flag-to-bit mapping (do not change this according to the README).
    const std::map<std::string, std::map<std::string, int>> detailedBitMapping = {
        {"CPV", { {"Bad", 0}, {"Invalid", 0} }},
        {"EMC", { {"Bad", 1}, {"NoDetectorData", 1}, {"BadEMCalorimetry", 1},
                  {"LimitedAcceptanceMCReproducible", 2} }},
        {"FDD", { {"Bad", 3}, {"Invalid", 3}, {"NoDetectorData", 3} }},
        {"FT0", { {"Bad", 4}, {"UnknownQuality", 4}, {"Unknown", 4} }},
        {"FV0", { {"Bad", 5} }},
        {"HMP", { {"Bad", 6}, {"NoDetectorData", 6} }},
        {"ITS", { {"Bad", 7}, {"UnknownQuality", 7}, {"BadTracking", 7},
                  {"LimitedAcceptanceMCReproducible", 8} }},
        {"MCH", { {"Bad", 9}, {"BadTracking", 9}, {"NoDetectorData", 9}, {"Unknown", 9},
                  {"LimitedAcceptanceMCReproducible", 10} }},
        {"MFT", { {"Bad", 11}, {"BadTracking", 11},
                  {"LimitedAcceptanceMCReproducible", 12} }},
        {"MID", { {"Bad", 13}, {"BadTracking", 13},
                  {"LimitedAcceptanceMCReproducible", 14} }},
        {"PHS", { {"Bad", 15}, {"Invalid", 15} }},
        {"TOF", { {"Bad", 16}, {"NoDetectorData", 16}, {"BadPID", 16},
                  {"LimitedAcceptanceMCReproducible", 17} }},
        {"TPC", { {"Bad", 18}, {"BadTracking", 18}, {"BadPID", 19},
                  {"LimitedAcceptanceMCNotReproducible", 18},
                  {"LimitedAcceptanceMCReproducible", 20} }},
        {"TRD", { {"Bad", 21}, {"BadTracking", 21} }},
        {"ZDC", { {"Bad", 22}, {"UnknownQuality", 22}, {"Unknown", 22},
                  {"NoDetectorData", 22} }}
    };

    // Open CSV file.
    std::ifstream file(csvFilePath);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << csvFilePath << std::endl;
        return;
    }

    // Read header and collect column names.
    std::string line;
    std::vector<std::string> columns;
    if (!std::getline(file, line)) {
        std::cerr << "Error: Failed to read header from file " << csvFilePath << std::endl;
        return;
    }
    std::istringstream headerStream(line);
    std::string column;
    std::cout << "pass" << std::endl;
    while (std::getline(headerStream, column, ',')) {
        column.erase(column.find_last_not_of(" \t\r\n") + 1);
        column.erase(std::remove(column.begin(), column.end(), '\r'), column.end());
        columns.push_back(column);
    }
    if (!columns.empty() && columns.back().empty()) {
        columns.pop_back();
        std::cerr << "Warning: Empty column detected in header. Removed." << std::endl;
    }

    // Open error log file.
    std::ofstream errorLog(getErrorLogFilename(), std::ios::app);
    if (!errorLog.is_open()) {
        std::cerr << "Error: Could not open error log file for writing." << std::endl;
        return;
    }

    // Initialize the CCDB API.
    o2::ccdb::CcdbApi ccdb;
    ccdb.init("http://alice-ccdb.cern.ch");
    std::cout << "pass2" << std::endl;

    // Container to accumulate all runs for the JSON output.
    std::map<std::string, std::map<uint64_t, uint32_t>> jsonRuns;
    uint64_t runStart;
    uint64_t runEnd;
    // Process each row in the CSV.
    while (std::getline(file, line)) {
        std::istringstream lineStream(line);
        std::vector<std::string> rowValues;
        std::string value;
        while (std::getline(lineStream, value, ',')) {
            value.erase(value.find_last_not_of(" \t\r\n") + 1);
            value.erase(std::remove(value.begin(), value.end(), '\r'), value.end());
            rowValues.push_back(value);
        }
        if (rowValues.size() != columns.size()) {
            std::cerr << "Error: Mismatch between header and row column count." << std::endl;
            continue;
        }

        std::string runNumber = rowValues[0];

        // Get run start (sor) and end (eor) times.
        auto runDuration = o2::ccdb::BasicCCDBManager::getRunDuration(ccdb, std::stoi(runNumber));
        uint64_t sor = runDuration.first;
        uint64_t eor = runDuration.second;
        runStart = sor;
	runEnd = eor;
        // Containers for time ranges and flag names.
        std::map<std::string, std::vector<std::pair<int64_t, int64_t>>> timeRanges;
        std::map<std::string, std::vector<std::pair<std::string, std::pair<int64_t, int64_t>>>> flagNames;
        std::set<std::string> skippedDetectors;

        // Process each detector (columns 1 and onward).
        for (size_t i = 1; i < rowValues.size(); ++i) {
            std::string detector = columns[i];
            std::string flags = rowValues[i];

            // Handle "Not present" and "Not Available" flags.
            if (flags == "Not present") {
                skippedDetectors.insert(detector); // Always bad.
                continue;
            } else if (flags == "Not Available") {
                if (!treatNotAvailableAsGood) {
                    skippedDetectors.insert(detector); // Bad when option is false.
                }
                continue;
            }

            // Use regex to extract flag name and time range.
            std::regex flagRegex(R"((\w+)\s+\(from:\s+(\d+)\s+to:\s+(\d+)\))");
            auto flagStart = std::sregex_iterator(flags.begin(), flags.end(), flagRegex);
            auto flagEnd = std::sregex_iterator();

            for (auto it = flagStart; it != flagEnd; ++it) {
                std::smatch match = *it;
                std::string flagName = match[1];
                int64_t from = std::stoll(match[2]);
                int64_t to = std::stoll(match[3]);

                // Log and treat unexpected flags as "Bad".
                if (!detailedBitMapping.at(detector).count(flagName) && flagName != "Good") {
                    errorLog << "Run: " << runNumber
                             << ", Detector: " << detector
                             << ", Unexpected Flag: " << flagName
                             << ", From: " << from
                             << ", To: " << to << "\n";
                    flagName = "Bad";
                }

                timeRanges[detector].emplace_back(from, to);
                flagNames[detector].emplace_back(flagName, std::make_pair(from, to));
            }
        }
	std::cout << "pass3" << std::endl;

        // Build the encoded flags map (one entry per distinct timestamp).
        std::map<uint64_t, uint32_t> encodedFlags;
        std::set<uint64_t> timePoints;
        for (const auto& [detector, ranges] : timeRanges) {
            for (const auto& range : ranges) {
                timePoints.insert(range.first);
                timePoints.insert(range.second);
            }
        }

       // If no detector gave any interval, timePoints will be empty
       if (timePoints.empty()) {
          // Define a single global segment for the run
          timePoints.insert(runStart);
          timePoints.insert(runEnd);
       } 

        std::vector<uint64_t> sortedTimePoints(timePoints.begin(), timePoints.end());
        std::sort(sortedTimePoints.begin(), sortedTimePoints.end());

        for (size_t i = 0; i < sortedTimePoints.size() - 1; ++i) {
            uint64_t fromTimestamp = sortedTimePoints[i];
            uint64_t toTimestamp = sortedTimePoints[i + 1];
            uint32_t encodedWord = 0;

            // Set bits for each detector if the flag is valid in this time range.
            for (const auto& [detector, ranges] : flagNames) {
                for (const auto& [flagName, range] : ranges) {
                    if (range.first <= fromTimestamp && range.second >= toTimestamp) {
                        if (detailedBitMapping.at(detector).count(flagName)) {
                            int bit = detailedBitMapping.at(detector).at(flagName);
                            encodedWord |= (1u << bit);
                        }
                    }
                }
            }

            // Always mark skipped detectors as "Bad".
            for (const auto& detector : skippedDetectors) {
                if (detailedBitMapping.at(detector).count("Bad")) {
                    int bit = detailedBitMapping.at(detector).at("Bad");
                    encodedWord |= (1u << bit);
                }
            }
            encodedFlags[fromTimestamp] = encodedWord;
        }

        // Squash consecutive entries with identical bitmasks.
        std::map<uint64_t, uint32_t> squashedEncodedFlags;
        if (!encodedFlags.empty()) {
            auto it = encodedFlags.begin();
            uint64_t lastTimestamp = it->first;
            uint32_t lastMask = it->second;
            squashedEncodedFlags[lastTimestamp] = lastMask;
            ++it;
            for (; it != encodedFlags.end(); ++it) {
                if (it->second != lastMask) {
                    squashedEncodedFlags[it->first] = it->second;
                    lastMask = it->second;
                }
            }
        }

        // Console summary.
        std::cout << "Run " << runNumber << " processed (" << squashedEncodedFlags.size()
                  << " segments).\n";

        // Add to JSON accumulator.
        jsonRuns[runNumber] = squashedEncodedFlags;

        // Prepare metadata.
        std::map<std::string, std::string> metadata;
        metadata["run"] = runNumber;
        metadata["passName"] = passName;
        metadata["periodName"] = periodName;
        metadata["version"] = versionNumber;

        // Upload the squashed encoded flags to CCDB.
        ccdb.storeAsTFileAny(&squashedEncodedFlags, ccdbPath, metadata,
                             sor - 10000, eor + 10000);
        std::cout << "  ↳ uploaded to CCDB\n";
    }
    errorLog.close();

    // -------------------------------------------------------------------------
    // Write ONE JSON file for the whole CSV.
    // -------------------------------------------------------------------------
    std::string csvName(csvFilePath);
    std::size_t pos = csvName.find_last_of("/\\");
    if (pos != std::string::npos) csvName = csvName.substr(pos + 1);
    pos = csvName.find_last_of('.');
    if (pos != std::string::npos) csvName = csvName.substr(0, pos);
    std::string jsonFileName = csvName + ".json";

    std::ofstream jsonOut(jsonFileName, std::ios::out | std::ios::trunc);
    if (!jsonOut.is_open()) {
        std::cerr << "Error: Could not write " << jsonFileName << std::endl;
        return;
    }

    jsonOut << "{\n";
    size_t runIdx = 0;
    for (const auto& [run, map] : jsonRuns) {
        jsonOut << "  \"" << run << "\": {\n";
        size_t segIdx = 0;
        for (const auto& [ts, mask] : map) {
            jsonOut << "    \"" << ts << "\": { "
                    << "\"dec\": " << mask << ", "
                    << "\"bin\": \"" << std::bitset<32>(mask).to_string() << "\" }"
                    << (segIdx + 1 < map.size() ? "," : "") << "\n";
            ++segIdx;
        }
        jsonOut << "  }" << (runIdx + 1 < jsonRuns.size() ? "," : "") << "\n";
        ++runIdx;
    }
    jsonOut << "  ,\"_meta\": { "
            << "\"csv\": \"" << csvName << "\", "
            << "\"pass\": \"" << passName << "\", "
            << "\"period\": \"" << periodName << "\", "
            << "\"version\": \"" << versionNumber << "\" }\n"
            << "}\n";
    jsonOut.close();

    std::cout << "Saved summary JSON: " << jsonFileName << std::endl;
}

