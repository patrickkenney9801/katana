#include "katana/ProgressTracer.h"

int
main() {
  katana::ProgressTracer::SetProgressTracer(katana::JsonTracer::Make());
  auto tracer = katana::ProgressTracer::GetProgressTracer();
  auto scope = tracer->StartActiveSpan("first span");
  scope.GetSpan()->SetTags(
      {{"life", static_cast<uint32_t>(42)},
       {"type", "test"},
       {"real", false},
       {"somethin", 2.0},
       {"hello", std::string{"world"}}});

  {
    auto list_scope = tracer->StartActiveSpan("reading query list");
    list_scope.GetSpan()->Log(
        "query parsed", {{"query", "CREATE"}, {"ops", 4}});

    for (auto i = 0; i < 2; i++) {
      auto query_scope = tracer->StartActiveSpan("running query");
      auto writing_scope = tracer->StartActiveSpan("writing query results");
    }
    auto span = tracer->StartSpan("is not initially active");
    auto span_scope = tracer->SetActiveSpan(span);
    auto no_raii_scope = tracer->StartActiveSpan("no raii", false);
  }
  tracer->GetActiveSpan()->Log("no raii is the active span");
  tracer->GetActiveSpan()->Finish();
  tracer->SetActiveSpan(nullptr);

  scope.Close();
  auto scope2 = tracer->StartActiveSpan("first span of second trace", false);
  std::string carrier = tracer->Inject(scope2.GetSpan()->GetContext());
  std::unique_ptr<katana::ProgressContext> ctx = tracer->Extract(carrier);
  scope2.GetSpan()->Log(
      "testing contexts",
      {{"trace_id", ctx->GetTraceId()}, {"span_id", ctx->GetSpanId()}});
  scope2.Close();  // this does nothing since open_on_close was set to false

  auto root_span = tracer->StartSpan("root span of new trace", true);
  auto scope3 = tracer->SetActiveSpan(root_span);
  tracer->GetActiveSpan()->Log("the new root span of trace 3 is active");

  auto scope2_child = tracer->StartActiveSpan(
      "child of trace 2's root by context", std::move(ctx));
  tracer->GetActiveSpan()->Log("child span of trace 2 is active");
  scope2_child.Close();
  root_span->Finish();
  scope2.GetSpan()->Finish();
}
