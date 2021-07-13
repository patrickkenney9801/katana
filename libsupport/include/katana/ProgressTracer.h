#ifndef KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_
#define KATANA_LIBSUPPORT_KATANA_PROGRESSTRACER_H_

#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "katana/config.h"

namespace katana {

// This is needed to properly handle being passed const char* values
// Once we submodule OpenTracing this will be removed and their Value class used instead
// TODO(Patrick)
using variant_type = std::variant<
    bool, int, int64_t, uint32_t, uint64_t, float, double, std::string,
    const char*>;
class Value : public variant_type {
public:
  Value(bool x) noexcept : variant_type(x) {}
  Value(int x) noexcept : variant_type(static_cast<int64_t>(x)) {}
  Value(int64_t x) noexcept : variant_type(x) {}
  Value(uint32_t x) noexcept : variant_type(static_cast<uint64_t>(x)) {}
  Value(uint64_t x) noexcept : variant_type(x) {}
  Value(double x) noexcept : variant_type(x) {}
  Value(std::string x) noexcept : variant_type(x) {}
  Value(const char* s) noexcept : variant_type(std::string(s)) {}
};
using Tags = std::vector<std::pair<std::string, Value>>;

class ProgressScope;
class ProgressSpan;
class ProgressContext;

// This class does not currently support thread-local tracers or concurrency controls
// Functions should only be used in a single-threaded context

// Best Practices:
//   If possible always avoid creating raw ProgressSpans from ProgressTracer
//   If possible always use ProgressScopes to handle ProgressSpans
//   Only use 1 ProgressTracer in an execution, so they should be created at entry points
//   Use ProgressScope's RAII to handle early returns (i.e. due to errors)
//   Raw ProgressSpans should be used for special scenarios like tracing asynchronous calls

// Notes:
//   SharedMemSys and DistMemSys will initialize the global ProgressTracer to a JsonTracer
//     later it will be set to a No-op Tracer, on Fini or the destructor they will call
//     Close on the global ProgressTracer
//   This prevents GetProgressTracer() from returning nullptr and ensures Tracers are closed
//
//   ProgressScope's will only close their ProgressSpan when their ProgressSpan is the active span
//   This means that if a user exclusively uses ProgressScopes,
//   Then parent ProgressSpans will not be closed until their child ProgressSpan's close
//   i.e.
//   {
//      auto scope1 = tracer->StartActiveSpan("1");
//      auto scope2 = tracer->StartActiveSpan("2");
//      if (err) { return; }
//      scope2.Close();
//      auto scope3 = tracer->StartActiveSpan("3");
//   }
//   Always results in scope2's ProgressSpan finishing before scope1's
//   and scope3's ProgressSpan finishing before scope1's if there is no error

class KATANA_EXPORT ProgressTracer {
  friend class ProgressSpan;

  void SetSpan(std::shared_ptr<ProgressSpan> span);

protected:
  ProgressTracer(uint32_t host_id, uint32_t num_hosts)
      : host_id_(host_id), num_hosts_(num_hosts) {}

public:
  virtual ~ProgressTracer() = default;

  static std::shared_ptr<ProgressTracer> GetProgressTracer() { return tracer_; }
  static void SetProgressTracer(std::shared_ptr<ProgressTracer> tracer);

  /// Create a new top level span if one does not already exist.
  /// Otherwise creates a child span of the current span
  /// Can specify to not finish the current scope’s span by RAII by
  /// setting finish_on_close=false
  ProgressScope StartActiveSpan(
      const std::string& span_name, bool finish_on_close = true);
  ProgressScope StartActiveSpan(
      const std::string& span_name,
      const std::unique_ptr<ProgressContext>& child_of,
      bool finish_on_close = true);

  /// Set the current active thread-local span to provided span
  /// Returns a ProgressScope
  /// Can specify to not finish the span when the scope goes out of scope
  /// by RAII by setting finish_on_close=false
  /// If called with a nullptr span,
  ///    calls StartActiveSpan("error: nullptr span given to SetActiveSpan", finish_on_close)
  ProgressScope SetActiveSpan(
      std::shared_ptr<ProgressSpan> span, bool finish_on_close = true);

  /// Create a new top level span if ignore_active_span=true and
  /// no child_of value is given
  /// Otherwise creates a child span of the child_of span or active span
  /// Should only be used to handle multiple active spans simultaneously
  virtual std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, bool ignore_active_span = false) = 0;
  virtual std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, std::shared_ptr<ProgressSpan> child_of) = 0;
  virtual std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      const std::unique_ptr<ProgressContext>& child_of) = 0;

  // For passing spans across process/host boundaries
  // These functions are needed, but are implemented now for
  // debugging purposes, they will be replaced
  // Extract returns a nullptr on failure
  virtual std::string Inject(const ProgressContext& ctx) = 0;
  virtual std::unique_ptr<ProgressContext> Extract(
      const std::string& carrier) = 0;

  /// Get the current scope’s span to add tagging/logging without having
  /// to pass around scopes/spans as parameters
  std::shared_ptr<ProgressSpan> GetActiveSpan() { return active_span_; }
  uint32_t GetHostId() { return host_id_; }
  uint32_t GetNumHosts() { return num_hosts_; }

  /// Close is called when a tracer is finished processing spans
  /// This should be called to ensure any and all buffered spans are
  /// flushed
  virtual void Close() = 0;

private:
  static std::shared_ptr<ProgressTracer> tracer_;

  std::shared_ptr<ProgressSpan> active_span_ = nullptr;
  uint32_t host_id_;
  uint32_t num_hosts_;
};

class KATANA_EXPORT ProgressScope {
  friend class ProgressTracer;

  ProgressScope(std::shared_ptr<ProgressSpan> span, bool finish_on_close)
      : span_(span), finish_on_close_(finish_on_close) {}

public:
  ~ProgressScope();
  ProgressScope(const ProgressScope&) = delete;
  ProgressScope(ProgressScope&&) = delete;
  ProgressScope& operator=(const ProgressScope&) = delete;
  ProgressScope& operator=(ProgressScope&&) = delete;

  /// Get the scope’s underlying span to add tagging/logging,
  /// span relationships, and handle multiple active spans at a time
  std::shared_ptr<ProgressSpan> GetSpan() { return span_; }

  /// Closes the underlying ProgressSpan if the ProgressScope was created
  /// with the flag finish_on_close=true
  /// Note that this will only close the underlying ProgressSpan when all
  /// of its active children ProgressSpans have been finished
  /// This will be called by RAII if it has not already been called
  void Close();

private:
  std::shared_ptr<ProgressSpan> span_;
  bool finish_on_close_ = false;
};

class KATANA_EXPORT ProgressContext {
public:
  virtual ~ProgressContext() = default;
  virtual std::unique_ptr<ProgressContext> Clone() const noexcept = 0;
  virtual std::string GetTraceId() const noexcept = 0;
  virtual std::string GetSpanId() const noexcept = 0;
};

class KATANA_EXPORT ProgressSpan {
  friend class ProgressTracer;
  friend class ProgressScope;

  void SetActive();
  void MarkScopeClosed();
  virtual void Close() = 0;

protected:
  ProgressSpan(std::shared_ptr<ProgressSpan> parent) : parent_(parent) {}

public:
  virtual ~ProgressSpan() = default;

  /// Adds a tag to the span.
  virtual void SetTags(const Tags& tags) = 0;

  /// Output logging as well as standard metrics and extra stats
  /// Current standard metrics: max_mem, mem, host, and timestamp.
  virtual void Log(const std::string& message, const Tags& tags = {}) = 0;

  /// Get span's context for propagating across process boundaries
  virtual const ProgressContext& GetContext() const noexcept = 0;

  bool ScopeClosed() { return scope_closed_; }

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
  void Finish();

private:
  std::shared_ptr<ProgressSpan> parent_ = nullptr;
  bool is_active_span_ = false;
  bool finished_ = false;
  bool scope_closed_ = false;
};

class KATANA_EXPORT JsonTracer : public ProgressTracer {
  JsonTracer(uint32_t host_id = 0, uint32_t num_hosts = 1)
      : ProgressTracer(host_id, num_hosts) {}

public:
  static std::shared_ptr<JsonTracer> Make(
      uint32_t host_id = 0, uint32_t num_hosts = 1);

  /// Create a new top level span if ignore_active_span=true and
  /// no child_of value is given
  /// Otherwise creates a child span of the child_of span or active span
  /// Should only be used to handle multiple active spans simultaneously
  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name, bool ignore_active_span = false) override;
  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      std::shared_ptr<ProgressSpan> child_of) override;
  std::shared_ptr<ProgressSpan> StartSpan(
      const std::string& span_name,
      const std::unique_ptr<ProgressContext>& child_of) override;

  std::string Inject(const ProgressContext& ctx) override;
  std::unique_ptr<ProgressContext> Extract(const std::string& carrier) override;

  /// Close is called when a tracer is finished processing spans
  /// This should be called to ensure any and all buffered spans are
  /// flushed
  void Close() override {}
};

class KATANA_EXPORT JsonContext : public ProgressContext {
  friend class JsonTracer;
  friend class JsonSpan;

  JsonContext(const std::string& trace_id, const std::string& span_id)
      : trace_id_(trace_id), span_id_(span_id) {}

public:
  std::unique_ptr<ProgressContext> Clone() const noexcept override {
    JsonContext ctx{trace_id_, span_id_};
    return std::make_unique<JsonContext>(ctx);
  }
  std::string GetTraceId() const noexcept override { return trace_id_; }
  std::string GetSpanId() const noexcept override { return span_id_; }

private:
  std::string trace_id_;
  std::string span_id_;
};

class KATANA_EXPORT JsonSpan : public ProgressSpan {
  friend JsonTracer;

  JsonSpan(
      const std::string& span_name,
      const std::shared_ptr<ProgressSpan>& parent);
  JsonSpan(
      const std::string& span_name,
      const std::unique_ptr<ProgressContext>& parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name,
      const std::shared_ptr<ProgressSpan>& parent);
  static std::shared_ptr<ProgressSpan> Make(
      const std::string& span_name,
      const std::unique_ptr<ProgressContext>& parent);

public:
  void Close() override;

  /// Adds a tag to the span.
  void SetTags(const Tags& tags) override;

  /// Output logging as well as standard metrics and extra stats
  /// Current standard metrics: max_mem, mem, host, and timestamp.
  void Log(const std::string& message, const Tags& tags = {}) override;

  const ProgressContext& GetContext() const noexcept override {
    return context_;
  }

private:
  JsonContext context_;
};

}  // namespace katana

#endif