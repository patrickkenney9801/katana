#include "galois/FileSystem.h"

#include "galois/Logging.h"

int
main() {
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path") == "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path/") == "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path//") == "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("/some/long/path///") == "path");
  GALOIS_LOG_ASSERT(galois::ExtractFileName("path") == "path");

  GALOIS_LOG_ASSERT(
      galois::ExtractDirName("/some/long/path").value() == "/some/long");
  GALOIS_LOG_ASSERT(
      galois::ExtractDirName("/some/long/path/").value() == "/some/long");
  GALOIS_LOG_ASSERT(
      galois::ExtractDirName("/some/long/path//").value() == "/some/long");
  GALOIS_LOG_ASSERT(
      galois::ExtractDirName("/some/long/path///").value() == "/some/long");

  GALOIS_LOG_ASSERT(
      galois::JoinPath("/some/long", "path") == "/some/long/path");
  GALOIS_LOG_ASSERT(
      galois::JoinPath("/some/long/", "path") == "/some/long/path");

  GALOIS_LOG_ASSERT(galois::StripURIScheme("/some/long/") == "/some/long/");
  GALOIS_LOG_ASSERT(
      galois::StripURIScheme("file:///some/long/") == "/some/long/");
  GALOIS_LOG_ASSERT(
      galois::StripURIScheme("s3://some//long///") == "some//long///");

  return 0;
}