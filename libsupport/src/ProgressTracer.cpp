#include "katana/ProgressTracer.h"

#include <sys/resource.h>
#include <sys/time.h>

#include <fstream>
#include <iostream>
#include <regex>

#include "katana/Time.h"

#if __linux__
#include <sys/sysinfo.h>
#endif

namespace {

const std::regex kRssRegex("^Rss\\w+:\\s+([0-9]+) kB");

std::once_flag printed_start;

#if __linux__

uint64_t
ParseProcSelfRssBytes() {
  std::ifstream proc_self("/proc/self/status");

  // there are 3 relevant vals: RssAnon, RssFile, RssShmem
  uint32_t rss_vals = 3;
  uint64_t total_mem = 0;
  std::string line;
  while (std::getline(proc_self, line) && rss_vals > 0) {
    std::smatch sub_match;
    if (!std::regex_match(line, sub_match, kRssRegex)) {
      continue;
    }
    std::string val = sub_match[1];
    total_mem += std::strtol(val.c_str(), nullptr, 0);
    rss_vals -= 1;
  }
  if (rss_vals != 0) {
    KATANA_LOG_ERROR("parsing /proc/self/status for memory failed");
  }
  return total_mem * 1024;
}

// returns pair of <number vCPUs, total RAM>
std::pair<long, long>
GetHostStats() {
  struct sysinfo info;
  sysinfo(&info);
  long ram = info.totalram / 1024 / 1024 / 1024;
  return std::pair<long, long>(get_nprocs(), ram);
}

#else

uint64_t
ParseProcSelfRssBytes() {
  KATANA_WARN_ONCE(
      "calculating resident set size is not implemented for this platform");
  return 0;
}

std::pair<int, int>
GetHostStats() {
  KATANA_WARN_ONCE("getting host stats is not implemented for this platform");
  return std::pair<int, int>(-1, -1);
}

#endif

// TODO (Patrick)
// This function is included for debugging purposes while
// the tracing infrastructure is added and tested
uint32_t id = 0;
std::string
GenerateId(uint32_t host_id) {
  id++;
  return std::to_string(host_id * 1000 + id);
}

std::string
GetValue(const katana::Value& value) {
  if (std::holds_alternative<std::string>(value)) {
    return "\"" + std::get<std::string>(value) + "\"";
  } else if (std::holds_alternative<int64_t>(value)) {
    return std::to_string(std::get<int64_t>(value));
  } else if (std::holds_alternative<double>(value)) {
    return std::to_string(std::get<double>(value));
  } else if (std::holds_alternative<bool>(value)) {
    return std::get<bool>(value) ? "true" : "false";
  } else if (std::holds_alternative<uint64_t>(value)) {
    return std::to_string(std::get<uint64_t>(value));
  }
  return std::string{};
}

std::string
GetSpanJson(
    const std::string& span_id, const std::string& span_name = std::string(),
    const std::string& parent_span_id = std::string()) {
  fmt::memory_buffer buf;
  if (span_name.empty() && parent_span_id.empty()) {
    fmt::format_to(
        std::back_inserter(buf), "\"span_data\":{{\"span_id\":\"{}\"}}",
        span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf),
        "\"span_data\":{{\"span_id\":\"{}\",\"span_name\":\"{}\",\"parent_id\":"
        "\"{}\"}}",
        span_id, span_name, parent_span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetSpanJson(const std::string& span_id, bool finish) {
  fmt::memory_buffer buf;
  if (finish) {
    fmt::format_to(
        std::back_inserter(buf),
        "\"span_data\":{{\"span_id\":\"{}\",\"finished\":\"true\"}}", span_id);
  } else {
    fmt::format_to(
        std::back_inserter(buf), "\"span_data\":{{\"span_id\":\"{}\"}}",
        span_id);
  }
  return fmt::to_string(buf);
}

std::string
GetHostStatsJson() {
  fmt::memory_buffer buf;
  static auto begin = katana::Now();
  std::pair<long, long> host_stats = GetHostStats();
  auto tracer = katana::ProgressTracer::GetProgressTracer();

  fmt::format_to(
      std::back_inserter(buf),
      "\"host_data\":{{\"hosts\":\"{}\",\"hardware_threads\":\"{}\",\"ram_gb\":"
      "\"{}\",\"epoch_time\":\"{}\"}}",
      tracer->GetNumHosts(), host_stats.first, host_stats.second,
      std::chrono::duration_cast<std::chrono::seconds>(begin.time_since_epoch())
          .count());

  return fmt::to_string(buf);
}

std::string
GetTagsJson(const katana::Tags& tags) {
  if (tags.empty()) {
    return std::string{};
  }
  fmt::memory_buffer buf;

  fmt::format_to(std::back_inserter(buf), "\"tags\":[");
  for (uint32_t i = 0; i < tags.size(); i++) {
    const std::pair<std::string, katana::Value>& tag = tags[i];

    if (i != 0) {
      fmt::format_to(std::back_inserter(buf), ",");
    }
    fmt::format_to(
        std::back_inserter(buf), "{{\"name\":\"{}\",\"value\":{}}}", tag.first,
        GetValue(tag.second));
  }
  fmt::format_to(std::back_inserter(buf), "]");
  return fmt::to_string(buf);
}

std::string
GetLogJson(const std::string& message) {
  fmt::memory_buffer buf;
  struct rusage rusage;
  getrusage(RUSAGE_SELF, &rusage);
  static auto begin = katana::Now();

  auto usec_since_begin = katana::UsSince(begin);
  fmt::format_to(
      std::back_inserter(buf),
      "\"log\":{{\"msg\":\"{}\",\"timestamp_ms\":\"{}\",\"max_mem_gb\"=\"{:.3f}"
      "\","
      "\"mem_gb\"=\"{:.3f}\"}}",
      message, usec_since_begin / 1000, rusage.ru_maxrss / 1024.0 / 1024.0,
      ParseProcSelfRssBytes() / 1024.0 / 1024.0 / 1024.0);

  return fmt::to_string(buf);
}

std::string
BuildJson(
    const std::string& trace_id, const std::string& span_data,
    const std::string& log_data, const std::string& tag_data,
    const std::string& host_data) {
  fmt::memory_buffer buf;
  uint32_t host_id = katana::ProgressTracer::GetProgressTracer()->GetHostId();

  fmt::format_to(
      std::back_inserter(buf), "{{\"trace_id\":\"{}\",\"host\":\"{}\",{}",
      trace_id, host_id, span_data);
  if (!log_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", log_data);
  }
  if (!tag_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", tag_data);
  }
  if (!host_data.empty()) {
    fmt::format_to(std::back_inserter(buf), ",{}", host_data);
  }
  fmt::format_to(std::back_inserter(buf), "}}\n");
  return fmt::to_string(buf);
}

}  // namespace

namespace katana {

std::shared_ptr<ProgressTracer> ProgressTracer::tracer_ = nullptr;

void
ProgressTracer::SetProgressTracer(std::shared_ptr<ProgressTracer> tracer) {
  ProgressTracer::tracer_ = tracer;
}

void
ProgressTracer::SetSpan(std::shared_ptr<ProgressSpan> span) {
  if (span != nullptr) {
    span->SetActive();
  }
  active_span_ = span;
  if (active_span_ != nullptr && active_span_->ScopeClosed()) {
    active_span_->Finish();
  }
}

/// Create a new top level span if one does not already exist.
/// Otherwise creates a child span of the current span
/// Can specify to not finish the current scope’s span by RAII by
/// setting finish_on_close=false
ProgressScope
ProgressTracer::StartActiveSpan(
    const std::string& span_name, bool finish_on_close) {
  return SetActiveSpan(StartSpan(span_name), finish_on_close);
}
ProgressScope
ProgressTracer::StartActiveSpan(
    const std::string& span_name,
    const std::unique_ptr<ProgressContext>& child_of, bool finish_on_close) {
  return SetActiveSpan(StartSpan(span_name, child_of), finish_on_close);
}

/// Set the current active thread-local span to provided span
/// Returns a ProgressScope
/// Can specify to not finish the span when the scope goes out of scope
/// by RAII by setting finish_on_close=false
ProgressScope
ProgressTracer::SetActiveSpan(
    std::shared_ptr<ProgressSpan> span, bool finish_on_close) {
  if (span == nullptr) {
    return StartActiveSpan(
        "error: nullptr span given to SetActiveSpan", finish_on_close);
  }
  span->SetActive();
  active_span_ = span;
  return ProgressScope(span, finish_on_close);
}

ProgressScope::~ProgressScope() {
  if (span_ != nullptr && finish_on_close_) {
    Close();
  }
}

/// Closes the underlying ProgressSpan if the ProgressScope was created
/// with the flag finish_on_close=true
/// Note that this will only close the underlying ProgressSpan when all
/// of its active children ProgressSpans have been finished
/// This will be called by RAII if it has not already been called
void
ProgressScope::Close() {
  if (finish_on_close_) {
    span_->MarkScopeClosed();
  }
}

void
ProgressSpan::MarkScopeClosed() {
  scope_closed_ = true;
  if (is_active_span_) {
    Finish();
  }
}

void
ProgressSpan::SetActive() {
  std::shared_ptr<ProgressSpan> active =
      ProgressTracer::GetProgressTracer()->GetActiveSpan();
  if (active != nullptr) {
    active->is_active_span_ = false;
  }
  is_active_span_ = true;
}

/// Every ProgressSpan started must be finished
///
/// If the ProgressScope for this ProgressSpan was created with
/// finish_on_close=true then this will be called via the
/// ProgressScope’s RAII if Close() has not already been called
///
/// If there is an unclosed ProgressSpan at the end of
/// execution then a warning will be given
/// Note that this immediately finishes the span regardless of it
/// having unfinished children ProgressSpans
void
ProgressSpan::Finish() {
  if (!finished_) {
    finished_ = true;
    Close();
    if (is_active_span_) {
      ProgressTracer::GetProgressTracer()->SetSpan(parent_);
    }
  }
}

std::shared_ptr<JsonTracer>
JsonTracer::Make(uint32_t host_id, uint32_t num_hosts) {
  JsonTracer tracer{host_id, num_hosts};
  return std::make_shared<JsonTracer>(std::move(tracer));
}

/// Create a new top level span if ignore_active_span=true and
/// no child_of value is given
/// Otherwise creates a child span of the child_of span or active span
/// Should only be used to handle multiple active spans simultaneously
std::shared_ptr<ProgressSpan>
JsonTracer::StartSpan(const std::string& span_name, bool ignore_active_span) {
  if (ignore_active_span) {
    return JsonSpan::Make(span_name, std::shared_ptr<JsonSpan>{nullptr});
  }
  return JsonSpan::Make(
      span_name, ProgressTracer::GetProgressTracer()->GetActiveSpan());
}
std::shared_ptr<ProgressSpan>
JsonTracer::StartSpan(
    const std::string& span_name, std::shared_ptr<ProgressSpan> child_of) {
  return JsonSpan::Make(span_name, child_of);
}
std::shared_ptr<ProgressSpan>
JsonTracer::StartSpan(
    const std::string& span_name,
    const std::unique_ptr<ProgressContext>& child_of) {
  return JsonSpan::Make(span_name, child_of);
}

std::string
JsonTracer::Inject(const ProgressContext& ctx) {
  return ctx.GetTraceId() + "," + ctx.GetSpanId();
}

std::unique_ptr<ProgressContext>
JsonTracer::Extract(const std::string& carrier) {
  size_t split = carrier.find(",");
  if (split == std::string::npos) {
    return nullptr;
  } else {
    std::string trace_id = carrier.substr(0, split);
    std::string span_id = carrier.substr(split + 1);
    JsonContext context{trace_id, span_id};
    return std::make_unique<JsonContext>(std::move(context));
  }
}

JsonSpan::JsonSpan(
    const std::string& span_name, const std::shared_ptr<ProgressSpan>& parent)
    : ProgressSpan(parent), context_(JsonContext{"", ""}) {
  auto tracer = ProgressTracer::GetProgressTracer();

  std::string parent_span_id{"null"};
  std::string trace_id;
  std::string host_data;
  if (parent != nullptr) {
    auto parent_span = std::static_pointer_cast<JsonSpan>(parent);
    parent_span_id = parent_span->GetContext().GetSpanId();
    trace_id = parent_span->GetContext().GetTraceId();
  } else {
    trace_id = GenerateId(tracer->GetHostId());
    host_data = GetHostStatsJson();
  }
  std::string span_id = GenerateId(tracer->GetHostId());
  context_ = JsonContext(trace_id, span_id);

  std::string message{"start"};

  std::string span_data = GetSpanJson(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJson(message);
  std::string tag_data;

  std::string output_json =
      BuildJson(trace_id, span_data, log_data, tag_data, host_data);

  std::cout << output_json;
}

JsonSpan::JsonSpan(
    const std::string& span_name,
    const std::unique_ptr<ProgressContext>& parent)
    : ProgressSpan(nullptr), context_(JsonContext{"", ""}) {
  auto tracer = ProgressTracer::GetProgressTracer();

  std::string parent_span_id{"null"};
  std::string trace_id;
  if (parent != nullptr) {
    parent_span_id = parent->GetSpanId();
    trace_id = parent->GetTraceId();
  } else {
    trace_id = GenerateId(tracer->GetHostId());
  }
  std::string span_id = GenerateId(tracer->GetHostId());
  context_ = JsonContext(trace_id, span_id);

  std::string message{"start"};

  std::string host_data = GetHostStatsJson();
  std::string span_data = GetSpanJson(span_id, span_name, parent_span_id);
  std::string log_data = GetLogJson(message);
  std::string tag_data;

  std::string output_json =
      BuildJson(trace_id, span_data, log_data, tag_data, host_data);

  std::cout << output_json;
}

std::shared_ptr<ProgressSpan>
JsonSpan::Make(
    const std::string& span_name, const std::shared_ptr<ProgressSpan>& parent) {
  JsonSpan span{span_name, parent};
  return std::make_shared<JsonSpan>(std::move(span));
}
std::shared_ptr<ProgressSpan>
JsonSpan::Make(
    const std::string& span_name,
    const std::unique_ptr<ProgressContext>& parent) {
  JsonSpan span{span_name, parent};
  return std::make_shared<JsonSpan>(std::move(span));
}

void
JsonSpan::Close() {
  std::string message{"finished"};

  std::string span_data = GetSpanJson(GetContext().GetSpanId(), true);
  std::string log_data = GetLogJson(message);
  std::string tag_data;
  std::string host_data;

  std::string output_json = BuildJson(
      GetContext().GetTraceId(), span_data, log_data, tag_data, host_data);

  std::cout << output_json;
}

/// Adds a tag to the span.
void
JsonSpan::SetTags(const Tags& tags) {
  std::string span_data = GetSpanJson(GetContext().GetSpanId());
  std::string log_data;
  std::string tag_data = GetTagsJson(tags);
  std::string host_data;

  std::string output_json = BuildJson(
      GetContext().GetTraceId(), span_data, log_data, tag_data, host_data);

  std::cout << output_json;
}

/// Output logging as well as standard metrics and extra stats
/// Current standard metrics: max_mem, mem, host, and timestamp.
void
JsonSpan::Log(const std::string& message, const Tags& tags) {
  std::string span_data = GetSpanJson(GetContext().GetSpanId());
  std::string log_data = GetLogJson(message);
  std::string tag_data = GetTagsJson(tags);
  std::string host_data;

  std::string output_json = BuildJson(
      GetContext().GetTraceId(), span_data, log_data, tag_data, host_data);

  std::cout << output_json;
}

}  // end of namespace katana
