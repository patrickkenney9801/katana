/*
 * This file belongs to the Galois project, a C++ library for exploiting parallelism.
 * The code is being released under the terms of the 3-Clause BSD License (a
 * copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#include "galois/Galois.h"
#include "galois/Reduction.h"
#include "galois/Bag.h"
#include "galois/Timer.h"
#include "galois/graphs/LCGraph.h"
#include "galois/ParallelSTL.h"
#include "llvm/Support/CommandLine.h"
#include "Lonestar/BoilerPlate.h"
#include "galois/runtime/Profile.h"
#include <boost/iterator/transform_iterator.hpp>

#define ENABLE_LABEL
#define USE_DOMAIN

const char* name = "FSM";
const char* desc = "Frequent subgraph mining in a graph using BFS expansion";
const char* url  = 0;

namespace cll = llvm::cl;
static cll::opt<std::string> filetype(cll::Positional, cll::desc("<filetype: txt,adj,mtx,gr>"), cll::Required);
static cll::opt<std::string> filename(cll::Positional, cll::desc("<filename: symmetrized graph>"), cll::Required);
static cll::opt<unsigned> k("k", cll::desc("max number of vertices in k-motif (default value 0)"), cll::init(0));
static cll::opt<unsigned> minsup("minsup", cll::desc("minimum suuport (default value 0)"), cll::init(0));
static cll::opt<unsigned> show("s", cll::desc("print out the details"), cll::init(0));
static cll::opt<unsigned> debug("d", cll::desc("print out the frequent patterns for debugging"), cll::init(0));
typedef galois::graphs::LC_CSR_Graph<uint32_t, uint32_t>::with_numa_alloc<true>::type ::with_no_lockable<true>::type Graph;
typedef Graph::GraphNode GNode;
int total_num = 0;

#define CHUNK_SIZE 256
#include "Mining/element.h"
typedef LabeledElement ElementType;
#include "Mining/embedding.h"
typedef EdgeEmbedding EmbeddingT;
typedef EdgeEmbeddingQueue EmbeddingQueueT;
#include "Mining/edge_miner.h"
#include "Mining/util.h"

#ifdef USE_DOMAIN
typedef DomainSupport SupportT;
typedef DomainMap SupportMap;
typedef QpMapDomain QpMapT;
typedef CgMapDomain CgMapT;
typedef LocalQpMapDomain LocalQpMapT;
typedef LocalCgMapDomain LocalCgMapT;
#else
typedef Frequency SupportT;
typedef FreqMap SupportMap;
typedef QpMapFreq QpMapT;
typedef CgMapFreq CgMapT;
typedef LocalQpMapFreq LocalQpMapT;
typedef LocalCgMapFreq LocalCgMapT;
#endif

// two-level aggregation
int aggregator(unsigned level, EdgeMiner& miner, EmbeddingQueueT& queue, CgMapT& cg_map) {
	if (show) std::cout << "\n---------------------------- Aggregating ----------------------------\n";
	cg_map.clear();

	QpMapT qp_map; // quick pattern map
	// quick aggregation
	LocalQpMapT qp_localmap; // quick pattern local map for each thread
	galois::do_all(galois::iterate(queue),
		[&](EmbeddingT &emb) {
			miner.quick_aggregate_each(emb, *(qp_localmap.getLocal()));
		},
		galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
		galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
		galois::loopname("QuickAggregation")
	);
	galois::StatTimer TmergeQP("MergeQuickPatterns");
	TmergeQP.start();
	miner.merge_qp_map(level+2, qp_localmap, qp_map);
	TmergeQP.stop();

	// canonical aggregation
	LocalCgMapT cg_localmap; // canonical pattern local map for each thread
	galois::do_all(galois::iterate(qp_map),
		[&](std::pair<QPattern, DomainSupport> element) {
			miner.canonical_aggregate_each(element.first, element.second, *(cg_localmap.getLocal()));
		},
		galois::chunk_size<CHUNK_SIZE>(), galois::steal(),
		galois::no_conflicts(), galois::wl<galois::worklists::PerSocketChunkFIFO<CHUNK_SIZE>>(),
		galois::loopname("CanonicalAggregation")
	);
	galois::StatTimer TmergeCG("MergeCanonicalPatterns");
	TmergeCG.start();
	miner.merge_cg_map(level+2, cg_localmap, cg_map);
	TmergeCG.stop();

	int num_frequent_patterns = 0;
	num_frequent_patterns = miner.support_count(cg_map);
	if (show) std::cout << "num_patterns: " << cg_map.size() << " num_quick_patterns: " << qp_map.size()
		<< " frequent patterns: " << num_frequent_patterns << "\n";
	return num_frequent_patterns;
}

void FsmSolver(EdgeMiner &miner) {
	unsigned level = 0;
	if (show) std::cout << "\n=============================== Start ===============================\n";
	EmbeddingQueueT queue, filtered_queue;

	CgMapT cg_map; // canonical graph map
	int num_freq_patterns = miner.init_aggregator();
	total_num += num_freq_patterns;
	if(num_freq_patterns == 0) {
		std::cout << "No frequent pattern found\n\n";
		return;
	}
	std::cout << "Number of frequent single-edge patterns: " << num_freq_patterns << "\n";
	if (debug) miner.printout_agg(cg_map);

	miner.init_filter(filtered_queue);
	if (show) filtered_queue.printout_embeddings(0);
	level ++;

	while (level < k) {
		if (show) std::cout << "\n============================== Level " << level << " ==============================\n";
		queue.clear();
		miner.expand_edge(k, filtered_queue, queue);
		if (show) queue.printout_embeddings(level, debug);

		num_freq_patterns = aggregator(level, miner, queue, cg_map);
		total_num += num_freq_patterns;
		if (debug) miner.printout_agg(cg_map);
		if (num_freq_patterns == 0) break;
		if (level == k-1) break;

		filtered_queue.clear();
		miner.filter(queue, filtered_queue);
		if (show) filtered_queue.printout_embeddings(level);
		level ++;
	}
	if (show) std::cout << "\n=============================== Done ================================\n";
	std::cout << "\n\tNumber of frequent patterns (minsup=" << minsup << "): " << total_num << "\n\n";
}

int main(int argc, char** argv) {
	galois::SharedMemSys G;
	LonestarStart(argc, argv, name, desc, url);
	Graph graph;
	galois::StatTimer Tinit("GraphReading");
	Tinit.start();
	read_graph(graph, filetype, filename);
	Tinit.stop();
	galois::gPrint("num_vertices ", graph.size(), " num_edges ", graph.sizeEdges(), "\n");

	EdgeMiner miner(&graph);
	miner.set_threshold(minsup);
	galois::StatTimer Tcomp("Compute");
	Tcomp.start();
	FsmSolver(miner);
	Tcomp.stop();
	return 0;
}
