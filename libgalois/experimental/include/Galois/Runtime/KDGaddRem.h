/** TODO -*- C++ -*-
 * @file
 * @section License
 *
 * This file is part of Galois.  Galoisis a framework to exploit
 * amorphous data-parallelism in irregular programs.
 *
 * Galois is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, version 2.1 of the
 * License.
 *
 * Galois is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Galois.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * @section Copyright
 *
 * Copyright (C) 2015, The University of Texas at Austin. All rights
 * reserved.
 *
 * @section Description
 *
 * TODO 
 *
 * @author <ahassaan@ices.utexas.edu>
 */
#ifndef GALOIS_RUNTIME_KDG_ADD_REM_H
#define GALOIS_RUNTIME_KDG_ADD_REM_H

#include "Galois/GaloisForwardDecl.h"
#include "Galois/Accumulator.h"
#include "Galois/Atomic.h"
#include "Galois/gdeque.h"
#include "Galois/PriorityQueue.h"
#include "Galois/Timer.h"
#include "Galois/AltBag.h"
#include "Galois/PerThreadContainer.h"

#include "Galois/Runtime/Context.h"
#include "Galois/Runtime/OrderedLockable.h"
#include "Galois/Runtime/Executor_DoAll.h"
#include "Galois/Runtime/Range.h"
#include "Galois/Runtime/ThreadRWlock.h"
#include "Galois/Runtime/Mem.h"
#include "Galois/Runtime/IKDGbase.h"

#include "Galois/WorkList/WorkList.h"

#include "Galois/gIO.h"

#include "llvm/ADT/SmallVector.h"

#include <iostream>
#include <unordered_map>

namespace Galois {
namespace Runtime {

namespace cll = llvm::cl;

static cll::opt<bool> addRemWinArg("addRemWin", cll::desc("enable windowing in add-rem executor"), cll::init(false));

static const bool debug = false;

template <typename Ctxt, typename CtxtCmp>
class NhoodItem: public OrdLocBase<NhoodItem<Ctxt, CtxtCmp>, Ctxt, CtxtCmp> {
  using Base = OrdLocBase<NhoodItem, Ctxt, CtxtCmp>;

public:
  // using PQ =  Galois::ThreadSafeOrderedSet<Ctxt*, CtxtCmp>;
  using PQ =  Galois::ThreadSafeMinHeap<Ctxt*, CtxtCmp>;
  using Factory = OrdLocFactoryBase<NhoodItem, Ctxt, CtxtCmp>;

protected:
  PQ sharers;

public:
  NhoodItem (Lockable* l, const CtxtCmp& ctxtcmp):  Base (l), sharers (ctxtcmp) {}

  void add (const Ctxt* ctxt) {

    assert (!sharers.find (const_cast<Ctxt*> (ctxt)));
    sharers.push (const_cast<Ctxt*> (ctxt));
  }

  bool isHighestPriority (const Ctxt* ctxt) const {
    assert (ctxt);
    assert (!sharers.empty ());
    return (sharers.top () == ctxt);
  }

  Ctxt* getHighestPriority () const {
    if (sharers.empty ()) { 
      return NULL;

    } else {
      return sharers.top ();
    }
  }

  void remove (const Ctxt* ctxt) {
    sharers.remove (const_cast<Ctxt*> (ctxt));
    // XXX: may fail in parallel execution
    assert (!sharers.find (const_cast<Ctxt*> (ctxt)));
  }

  void print () const { 
    // TODO
  }
};


template <typename T, typename Cmp>
class KDGaddRemContext: public OrderedContextBase<T> {

public:
  typedef T value_type;
  typedef KDGaddRemContext MyType;
  typedef ContextComparator<MyType, Cmp> CtxtCmp; 
  typedef NhoodItem<MyType, CtxtCmp> NItem;
  typedef PtrBasedNhoodMgr<NItem> NhoodMgr;
  typedef Galois::GAtomic<bool> AtomicBool;
  // typedef Galois::gdeque<NItem*, 4> NhoodList;
  // typedef llvm::SmallVector<NItem*, 8> NhoodList;
  typedef typename gstl::Vector<NItem*> NhoodList;
  // typedef std::vector<NItem*> NhoodList;

  // TODO: fix visibility below
public:
  // FIXME: nhood should be a set instead of list
  // AtomicBool onWL;
  NhoodMgr& nhmgr;
  NhoodList nhood;
  // GALOIS_ATTRIBUTE_ALIGN_CACHE_LINE AtomicBool onWL;
  AtomicBool onWL;

public:

  KDGaddRemContext (const T& active, NhoodMgr& nhmgr)
    : 
      OrderedContextBase<T> (active), // to make acquire call virtual function sub_acquire
      nhmgr (nhmgr),
      onWL (false)
  {}

  GALOIS_ATTRIBUTE_PROF_NOINLINE
  virtual void subAcquire (Lockable* l, Galois::MethodFlag) {
    NItem& nitem = nhmgr.getNhoodItem (l);

    assert (NItem::getOwner (l) == &nitem);

    if (std::find (nhood.begin (), nhood.end (), &nitem) == nhood.end ()) {
      nhood.push_back (&nitem);
      nitem.add (this);
    }
    
  }

  GALOIS_ATTRIBUTE_PROF_NOINLINE bool isSrc () const {
    bool ret = true;

    for (auto n = nhood.begin () , endn = nhood.end (); n != endn; ++n) {
      if (!(*n)->isHighestPriority (this)) {
        ret = false;
        break;
      }
    }

    return ret;
  }

  GALOIS_ATTRIBUTE_PROF_NOINLINE void removeFromNhood () {
    for (auto n = nhood.begin ()
        , endn = nhood.end (); n != endn; ++n) {

      (*n)->remove (this);
    }
  }

// for DEBUG
  std::string str () const {
    std::stringstream ss;
#if 0
    ss << "[" << this << ": " << active << "]";
#endif 
    return ss.str ();
  }

  template <typename SourceTest, typename WL>
  GALOIS_ATTRIBUTE_PROF_NOINLINE void findNewSources (const SourceTest& srcTest, WL& wl) {

    for (auto n = nhood.begin ()
        , endn = nhood.end (); n != endn; ++n) {

      KDGaddRemContext* highest = (*n)->getHighestPriority ();
      if ((highest != NULL) 
          && !bool (highest->onWL)
          && srcTest (highest) 
          && highest->onWL.cas (false, true)) {

        // GALOIS_DEBUG ("Adding found source: %s\n", highest->str ().c_str ());
        wl.push (highest);
      }
    }
  }

  // TODO: combine with above method and reuse the code
  template <typename SourceTest, typename WL>
  GALOIS_ATTRIBUTE_PROF_NOINLINE void findSrcInNhood (const SourceTest& srcTest, WL& wl) {

    for (auto n = nhood.begin ()
        , endn = nhood.end (); n != endn; ++n) {

      KDGaddRemContext* highest = (*n)->getHighestPriority ();
      if ((highest != NULL) 
          && !bool (highest->onWL)
          && srcTest (highest) 
          && highest->onWL.cas (false, true)) {

        // GALOIS_DEBUG ("Adding found source: %s\n", highest->str ().c_str ());
        wl.push_back (highest);
      }
    }
  }

  void disableSrc (void) const {
    // XXX: nothing to do here. Added to reuse runCatching
  }


};



template <typename StableTest>
struct SourceTest {

  StableTest stabilityTest;

  explicit SourceTest (const StableTest& stabilityTest)
    : stabilityTest (stabilityTest) {}

  template <typename Ctxt>
  bool operator () (const Ctxt* ctxt) const {
    assert (ctxt != NULL);
    return ctxt->isSrc () && stabilityTest (ctxt->getActive());
  }
};

template <>
struct SourceTest <void> {

  template <typename Ctxt>
  bool operator () (const Ctxt* ctxt) const {
    assert (ctxt != NULL);
    return ctxt->isSrc ();
  }
};

// TODO: remove template parameters that can be passed to execute
template <typename T, typename Cmp, typename NhFunc, typename OpFunc, typename SourceTest, typename ArgsTuple, typename Ctxt>
class KDGaddRemAsyncExec: public IKDGbase<T, Cmp, NhFunc, HIDDEN::DummyExecFunc, OpFunc, ArgsTuple, Ctxt> {

  using ThisClass = KDGaddRemAsyncExec;

protected:
  using Base = IKDGbase<T, Cmp, NhFunc, HIDDEN::DummyExecFunc, OpFunc, ArgsTuple, Ctxt>;

  // important paramters
  // TODO: add capability to the interface to express these constants
  static const size_t DELETE_CONTEXT_SIZE = 1024;
  static const size_t UNROLL_FACTOR = OpFunc::UNROLL_FACTOR;

  static const unsigned DEFAULT_CHUNK_SIZE = 8;

  typedef Galois::WorkList::dChunkedFIFO<OpFunc::CHUNK_SIZE, Ctxt*> SrcWL_ty;
  // typedef Galois::WorkList::AltChunkedFIFO<CHUNK_SIZE, Ctxt*> SrcWL_ty;


  // typedef MapBasedNhoodMgr<T, Cmp> NhoodMgr;
  // typedef NhoodItem<T, Cmp, NhoodMgr> NItem;
  // typedef typename NItem::Ctxt Ctxt;

  using NhoodMgr =  typename Ctxt::NhoodMgr;

  using CtxtAlloc = typename Base::CtxtAlloc;
  using CtxtWL = typename Base::CtxtWL;

  using CtxtDelQ = PerThreadDeque<Ctxt*>; // XXX: can also use gdeque
  using CtxtLocalQ = PerThreadDeque<Ctxt*>;

  using UserCtxt = typename Base::UserCtxt;
  using PerThreadUserCtxt = typename Base::PerThreadUserCtxt;


  using Accumulator =  Galois::GAccumulator<size_t>;

  struct CreateCtxtExpandNhood {
    KDGaddRemAsyncExec& exec;
    Accumulator& nInit;

    GALOIS_ATTRIBUTE_PROF_NOINLINE void operator () (const T& active) const {

      Ctxt* ctxt = exec.ctxtAlloc.allocate (1);
      assert (ctxt != NULL);
      // new (ctxt) Ctxt (active, nhmgr);
      //ctxtAlloc.construct (ctxt, Ctxt (active, nhmgr));
      exec.ctxtAlloc.construct (ctxt, active, exec.nhmgr);

      exec.addCtxtWL.get ().push_back (ctxt);

      nInit += 1;

      auto& uhand = *(exec.userHandles.getLocal ());
      runCatching (exec.nhFunc, ctxt, uhand);
    }

  };

  struct DummyWinWL {
    void push (const T&) const {
      std::abort ();
    }
  };


  template <typename WinWL>
  struct ApplyOperator {
    static const bool USE_WIN_WL = !std::is_same<WinWL, DummyWinWL>::value;
    typedef int tt_does_not_need_aborts;

    KDGaddRemAsyncExec& exec;
    WinWL& winWL;
    const Galois::optional<T>& minWinWL;
    Accumulator& nsrc;
    Accumulator& ntotal;;

    template <typename WL>
    GALOIS_ATTRIBUTE_PROF_NOINLINE void operator () (Ctxt* const in_src, WL& wl) {
      assert (in_src != NULL);

      exec.ctxtLocalQ.get ().clear ();

      exec.ctxtLocalQ.get ().push_back (in_src);


      for (unsigned local_iter = 0; 
          (local_iter < UNROLL_FACTOR) && !exec.ctxtLocalQ.get ().empty (); ++local_iter ) {

        Ctxt* src = exec.ctxtLocalQ.get ().front (); exec.ctxtLocalQ.get ().pop_front ();

        // GALOIS_DEBUG ("Processing source: %s\n", src->str ().c_str ());
        if (debug && !exec.sourceTest (src)) {
          std::cout << "Not found to be a source: " << src->str ()
            << std::endl;
          // abort ();
        }

        nsrc += 1;

        // addWL.get ().clear ();
        UserCtxt& userCtxt = *(exec.userHandles.getLocal ());

        if (ThisClass::NEEDS_PUSH) {
          userCtxt.resetPushBuffer ();
        }

        exec.opFunc (src->getActive (), userCtxt); 


        if (ThisClass::NEEDS_PUSH) {

          exec.addCtxtWL.get ().clear ();
          CreateCtxtExpandNhood addCtxt {exec, ntotal};


          for (auto a = userCtxt.getPushBuffer ().begin ()
              , enda = userCtxt.getPushBuffer ().end (); a != enda; ++a) {

            if (!USE_WIN_WL || !minWinWL || exec.cmp (*a, *minWinWL)) {
              addCtxt (*a);
            } else {
              winWL.push (*a);
            }
  
          }

          for (auto c = exec.addCtxtWL.get ().begin ()
              , endc = exec.addCtxtWL.get ().end (); c != endc; ++c) {

            (*c)->findNewSources (exec.sourceTest, wl);
            // // if is source add to workList;
            // if (sourceTest (*c) && (*c)->onWL.cas (false, true)) {
            // // std::cout << "Adding new source: " << *c << std::endl;
            // wl.push (*c);
            // }
          }
        }

        src->removeFromNhood ();

        src->findSrcInNhood (exec.sourceTest, exec.ctxtLocalQ.get ());

        //TODO: use a ref count type wrapper for Ctxt;
        exec.ctxtDelQ.get ().push_back (src);

      }

      // add remaining to global wl
      // TODO: check onWL counter here
      for (auto c = exec.ctxtLocalQ.get ().begin ()
          , endc = exec.ctxtLocalQ.get ().end (); c != endc; ++c) {

        wl.push (*c);
      }

      while (exec.ctxtDelQ.get ().size () >= DELETE_CONTEXT_SIZE) {

        Ctxt* c = exec.ctxtDelQ.get ().front (); exec.ctxtDelQ.get ().pop_front ();
        exec.ctxtAlloc.destroy (c);
        exec.ctxtAlloc.deallocate (c, 1); 
      }
    }

  };

private:
  NhoodMgr& nhmgr;
  SourceTest sourceTest;
  CtxtWL addCtxtWL;
  CtxtLocalQ ctxtLocalQ;
  CtxtDelQ ctxtDelQ;


public:

  KDGaddRemAsyncExec (
      const Cmp& cmp,
      const NhFunc& nhFunc,
      const OpFunc& opFunc,
      const ArgsTuple& argsTuple, 
      NhoodMgr& nhmgr,
      const SourceTest& sourceTest)
    :
      Base (cmp, nhFunc, HIDDEN::DummyExecFunc (), opFunc, argsTuple),
      nhmgr (nhmgr),
      sourceTest (sourceTest)
  {}


  template <typename R>
  void expandNhoodpickSources (const R& range, CtxtWL& sources, Accumulator& nInit) {

    addCtxtWL.clear_all_parallel ();

    Galois::do_all_choice (
        range, 
        CreateCtxtExpandNhood {*this, nInit},
        std::make_tuple (
          Galois::loopname ("create_contexts"),
          Galois::chunk_size<NhFunc::CHUNK_SIZE> ()));

    Galois::do_all_choice (makeLocalRange(this->addCtxtWL),
        [this, &sources] (Ctxt* ctxt) {
          if (sourceTest (ctxt) && ctxt->onWL.cas (false, true)) {
            sources.get ().push_back (ctxt);
          }
        },
        std::make_tuple (
          Galois::loopname ("find_sources"),
          Galois::chunk_size<DEFAULT_CHUNK_SIZE> ()));

  }

  template <typename A>
  void applyOperator (CtxtWL& sources, A op) {

    Galois::for_each_local(sources,
        op,
        Galois::loopname("apply_operator"), Galois::wl<SrcWL_ty>());

    Galois::do_all_choice (makeLocalRange(ctxtDelQ),
        [this] (Ctxt* ctxt) {
          ThisClass::ctxtAlloc.destroy (ctxt);
          ThisClass::ctxtAlloc.deallocate (ctxt, 1);
        },
        std::make_tuple (
          Galois::loopname ("delete_all_ctxt"),
          Galois::chunk_size<DEFAULT_CHUNK_SIZE> ()));

    Runtime::on_each_impl (
        [this, &sources] (const unsigned tid, const unsigned numT) {
          sources.get ().clear ();
          ctxtDelQ.get ().clear ();
        });

  }

  template <typename R>
  void execute (const R& range) {
    CtxtWL initSrc;

    Accumulator nInitCtxt;
    Accumulator nsrc;

    Galois::TimeAccumulator t_create;
    Galois::TimeAccumulator t_find;
    Galois::TimeAccumulator t_for;
    Galois::TimeAccumulator t_destroy;

    t_create.start ();
    expandNhoodpickSources (range, initSrc, nInitCtxt);
    t_create.stop ();

    // TODO: code to find global min goes here

    DummyWinWL winWL;
    Galois::optional<T> minWinWL; // should remain uninitialized 

    t_for.start ();
    applyOperator (initSrc, ApplyOperator<DummyWinWL> {*this, winWL, minWinWL, nsrc, nInitCtxt});
    t_for.stop ();

    reportStat (ThisClass::loopname, "Number of iterations: ", nsrc.reduce (), 0);
    reportStat (ThisClass::loopname, "Time taken in creating intial contexts: ",   t_create.get (), 0);
    reportStat (ThisClass::loopname, "Time taken in finding intial sources: ", t_find.get (), 0);
    reportStat (ThisClass::loopname, "Time taken in for_each loop: ", t_for.get (), 0);
    reportStat (ThisClass::loopname, "Time taken in destroying all the contexts: ", t_destroy.get (), 0);
  }
};


template <typename T, typename Cmp, typename NhFunc, typename OpFunc, typename SourceTest, typename ArgsTuple, typename Ctxt> 
class KDGaddRemWindowExec: public KDGaddRemAsyncExec<T, Cmp, NhFunc, OpFunc, SourceTest, ArgsTuple, Ctxt> {

  using Base = KDGaddRemAsyncExec<T, Cmp, NhFunc, OpFunc, SourceTest, ArgsTuple, Ctxt>;

  using WindowWL = typename std::conditional<Base::NEEDS_PUSH, PQwindowWL<T, Cmp>, SortedRangeWindowWL<T, Cmp> >::type;

  using CtxtWL = typename Base::CtxtWL;
  using Accumulator = typename Base::Accumulator;

  using ThisClass = KDGaddRemWindowExec;


  WindowWL winWL;
  PerThreadBag<T> pending;
  CtxtWL sources;
  Accumulator nsrc;

  void beginRound (void) {
    ThisClass::refillRound (winWL, pending);
  }

  void expandNhoodPending (void) {

    ThisClass::expandNhoodpickSources (makeLocalRange (pending), sources, ThisClass::roundTasks);
    pending.clear_all_parallel ();

  }

  void applyOperator (void) {

    Galois::optional<T> minWinWL;

    if (ThisClass::NEEDS_PUSH && ThisClass::targetCommitRatio != 0.0) {
      minWinWL = winWL.getMin ();
    }
    
    using Op = typename ThisClass::template ApplyOperator<WindowWL>;
    Base::applyOperator (sources, Op {*this, winWL, minWinWL, ThisClass::roundCommits, ThisClass::roundTasks});
  }

  template <typename R>
  void push_initial (const R& range) {

    if (ThisClass::targetCommitRatio == 0.0) {

      Galois::do_all_choice (range,
          [this] (const T& x) {
            pending.push (x);
          }, 
          std::make_tuple (
            Galois::loopname ("init-fill"),
            chunk_size<NhFunc::CHUNK_SIZE> ()));


    } else {
      winWL.initfill (range);
    }
  }

public:

  KDGaddRemWindowExec (
      const Cmp& cmp,
      const NhFunc& nhFunc,
      const OpFunc& opFunc,
      const ArgsTuple& argsTuple, 
      typename Base::NhoodMgr& nhmgr,
      const SourceTest& sourceTest)
    :
      Base (cmp, nhFunc, opFunc, argsTuple, nhmgr, sourceTest)
  {}

  template <typename R>
  void execute (const R& range) {

    push_initial (range);

    while (true) {

      beginRound ();

      expandNhoodPending ();

      if (sources.empty_all ()) {
        assert (pending.empty_all ());
        break;
      }


      applyOperator ();

      ThisClass::endRound ();


    }
  }

};


template <
    template <typename _one, typename _two, typename _three, typename _four, typename _five, typename _six, typename _seven> class Executor, 
    typename R, typename Cmp, typename NhFunc, typename OpFunc, typename ST, typename ArgsTuple>
void for_each_ordered_ar_impl (const R& range, const Cmp& cmp, const NhFunc& nhFunc, const OpFunc& opFunc, const ST& sourceTest, const ArgsTuple& argsTuple) {

  typedef typename R::value_type T;

  auto argsT = std::tuple_cat (argsTuple, 
      get_default_trait_values (argsTuple,
        std::make_tuple (loopname_tag {}, enable_parameter_tag {}),
        std::make_tuple (default_loopname {}, enable_parameter<false> {})));
  using ArgsT = decltype (argsT);

  typedef KDGaddRemContext<T, Cmp> Ctxt;
  typedef typename Ctxt::NhoodMgr NhoodMgr;
  typedef typename Ctxt::NItem NItem;
  typedef typename Ctxt::CtxtCmp  CtxtCmp;

  using Exec =  Executor<T, Cmp, NhFunc, OpFunc, ST, ArgsT, Ctxt>;

  std::cout << "sizeof(KDGaddRemContext) == " << sizeof(Ctxt) << std::endl;

  CtxtCmp ctxtcmp (cmp);
  typename NItem::Factory factory(ctxtcmp);
  NhoodMgr nhmgr (factory);

  Exec e (cmp, nhFunc, opFunc, argsT, nhmgr, sourceTest);
  e.execute (range);
}

template <typename R, typename Cmp, typename NhFunc, typename OpFunc, typename StableTest, typename ArgsTuple>
void for_each_ordered_ar (const R& range, const Cmp& cmp, const NhFunc& nhFunc, const OpFunc& opFunc, const StableTest& stabilityTest, const ArgsTuple& argsTuple) {

  if (addRemWinArg) {
    for_each_ordered_ar_impl<KDGaddRemWindowExec> (range, cmp, nhFunc, opFunc, SourceTest<StableTest> (stabilityTest), argsTuple);
  } else {
    for_each_ordered_ar_impl<KDGaddRemAsyncExec> (range, cmp, nhFunc, opFunc, SourceTest<StableTest> (stabilityTest), argsTuple);
  }
}

template <typename R, typename Cmp, typename NhFunc, typename OpFunc, typename ArgsTuple>
void for_each_ordered_ar (const R& range, const Cmp& cmp, const NhFunc& nhFunc, const OpFunc& opFunc, const ArgsTuple& argsTuple) {

  if (addRemWinArg) {
    for_each_ordered_ar_impl<KDGaddRemWindowExec> (range, cmp, nhFunc, opFunc, SourceTest<void> (), argsTuple);
  } else {
    for_each_ordered_ar_impl<KDGaddRemAsyncExec> (range, cmp, nhFunc, opFunc, SourceTest<void> (), argsTuple);
  }
}

} // end namespace Runtime
} // end namespace Galois

#endif //  GALOIS_RUNTIME_KDG_ADD_REM_H