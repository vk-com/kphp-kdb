/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption 
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#pragma once
#include "../common.h"

extern volatile int bicycle_counter;

const int MAX_THREADS_COUNT = 101;
int get_thread_id();
void set_thread_id (int new_thread_id);

template <class T> bool try_lock (T);

template <class T> void lock (T x) {
  x->lock();
}
template <class T> void unlock (T x) {
  x->unlock();
}

template <>
inline bool try_lock <volatile int *> (volatile int *locker) {
  return __sync_lock_test_and_set (locker, 1) == 0;
}

template <>
inline void lock<volatile int *> (volatile int *locker) {
  while (!try_lock (locker)) {
    usleep (250);
  }
}

template <>
inline void unlock<volatile int *> (volatile int *locker) {
  assert (*locker == 1);
  __sync_lock_release (locker);
}

class Lockable {
  private:
    volatile int x;
  public:
    Lockable() : x(0) {}
    virtual ~Lockable(){}
    void lock() {
      ::lock (&x);
    }
    void unlock() {
      ::unlock (&x);
    }
};

template <class DataT>
class AutoLocker {
  private:
    DataT ptr;
  public:
    inline AutoLocker (DataT ptr) :
      ptr (ptr) {
        lock (ptr);
      }

    inline ~AutoLocker() {
      unlock (ptr);
    }
};

template <int x = 0>
class Nothing_ {
};
typedef Nothing_<> Nothing;

template <class ValueType>
class Maybe {
  public:
    bool has_value;
    char data[sizeof (ValueType)];

    Maybe() :
      has_value (false) {
      }

    Maybe (Nothing x) :
      has_value (false) {
      }

    Maybe (const ValueType &value) {
      has_value = true;
      new (data) ValueType (value);
    }

    operator const ValueType &() const {
      assert (has_value);
      return *(ValueType *)data;
    }

    bool empty() {
      return !has_value;
    }
};

inline int atomic_int_inc (volatile int *x) {
  int old_x;
  do {
    old_x = *x;
  } while (!__sync_bool_compare_and_swap (x, old_x, old_x + 1));
  return old_x;
}

inline void atomic_int_dec (volatile int *x) {
  int old_x;
  do {
    old_x = *x;
  } while (!__sync_bool_compare_and_swap (x, old_x, old_x - 1));
}

template <class T>
struct TLS {
  private:
    struct TLSRaw {
      T data;
      volatile int locker;
      char dummy[4096];
      TLSRaw() :
        data(),
        locker (0) {
      }
    };
    TLSRaw arr[MAX_THREADS_COUNT + 1];
  public:
    TLS() :
      arr() {
    }
    TLSRaw *get_raw (int id) {
      assert (0 <= id && id <= MAX_THREADS_COUNT);
      return &arr[id];
    }
    TLSRaw *get_raw() {
      return get_raw (get_thread_id());
    }

    T *get() {
      return &get_raw()->data;
    }
    T *get (int i) {
      return &get_raw (i)->data;
    }

    T *operator -> () {
      return get();
    }
    T &operator * () {
      return *get();
    }

    int size() {
      return MAX_THREADS_COUNT + 1;
    }

    T *lock_get() {
      TLSRaw *raw = get_raw();
      bool ok = try_lock (&raw->locker);
      assert (ok);
      return &raw->data;
    }

    void unlock_get (T *ptr) {
      TLSRaw *raw = get_raw();
      assert (&raw->data == ptr);
      unlock (&raw->locker);
    }
};

class Task {
  public:
    virtual void execute() = 0;
    virtual ~Task(){}
};

inline void execute_task (Task *task) {
  task->execute();
  delete task;
  atomic_int_dec (&bicycle_counter);
}

class Node;
class SchedulerBase {
  public:
    SchedulerBase();
    virtual ~SchedulerBase();
    virtual void add_node (Node *node) = 0;
    virtual void add_sync_node (Node *node) = 0;
    virtual void add_task (Task *task) = 0;
    virtual void execute() = 0;
};

SchedulerBase *get_scheduler();
void set_scheduler (SchedulerBase *new_scheduler);
void unset_scheduler (SchedulerBase *old_scheduler);


inline void register_async_task (Task *task) {
  get_scheduler()->add_task (task);
}

class Node {
  public:
    SchedulerBase *in_scheduler;
    bool parallel;
    bool sync_node;
    Node (bool parallel = true, bool sync_node = false) :
     in_scheduler (NULL),
     parallel (parallel),
     sync_node (sync_node) {
    }
    virtual ~Node(){}
    void add_to_scheduler (SchedulerBase *scheduler) {
      if (in_scheduler == scheduler) {
        return;
      }
      assert (in_scheduler == NULL);
      in_scheduler = scheduler;
      in_scheduler->add_node (this);
    }
    virtual bool is_parallel() {
      return parallel;
    }
    virtual bool is_sync_node() {
      return sync_node;
    }
    virtual Task *get_task() = 0;
    virtual void on_finish() = 0;
};
#define DUMMY_ON_FINISH template <class OutputStreamT> void on_finish (OutputStreamT &os) {}

class TaskPull;
class OneThreadScheduler : public SchedulerBase {
  private:
    vector <Node *> nodes;
    queue <Node *> sync_nodes;
    TaskPull *task_pull;
  public:
    OneThreadScheduler();

    void add_node (Node *node);
    void add_task (Task *task);
    void add_sync_node (Node *node);
    void execute();
    void set_threads_count (int threads_count);
};

class Scheduler;
class ThreadLocalStorage {
  public:
    pthread_t pthread_id;
    int thread_id;
    Scheduler *scheduler;
    Node *node;
    bool run_flag;
    double worked;
    double started;
    double finished;
};

void *thread_execute (void *arg);
class Scheduler : public SchedulerBase {
  private:
    vector <Node *> nodes;
    vector <Node *> one_thread_nodes;
    queue <Node *> sync_nodes;
    int threads_count;
    TaskPull *task_pull;

  public:
    Scheduler();

    void add_node (Node *node);
    void add_task (Task *task);
    void add_sync_node (Node *node);
    void execute();

    void set_threads_count (int new_threads_count);
    void thread_execute (ThreadLocalStorage *tls);
    bool thread_process_node (ThreadLocalStorage *tls, Node *node);
};
template <class DataT>
class DataStreamRaw : Lockable {
  private:
    DataT *data;
    volatile int *ready;
    volatile int write_i, read_i;
    bool sink;
  public:
    typedef DataT DataType;
    DataStreamRaw() :
      write_i (0),
      read_i (0),
      sink (false) {
      //FIXME
      data =  new DataT[50000]();
      ready = new int[50000]();
    }
    bool empty() {
      return read_i == write_i;
    }
    Maybe <DataT> get() {
      //AutoLocker <Lockable *> (this);
      while (true) {
        int old_read_i = read_i;
        if (old_read_i < write_i) {
          if (__sync_bool_compare_and_swap (&read_i, old_read_i, old_read_i + 1)) {
            while (!ready[old_read_i]) {
              usleep (250);
            }
            DataT result;
            std::swap (result, data[old_read_i]);
            return result;
          }
          usleep (250);
        } else {
          return Nothing();
        }
      }
      return Nothing();
    }
    void operator << (const DataType &input) {
      //AutoLocker <Lockable *> (this);
      if (!sink) {
        atomic_int_inc (&bicycle_counter);
      }
      while (true) {
        int old_write_i = write_i;
        assert (old_write_i < 50000);
        if (__sync_bool_compare_and_swap (&write_i, old_write_i, old_write_i + 1)) {
          data[old_write_i] = input;
          __sync_synchronize();
          ready[old_write_i] = 1;
          return;
        }
        usleep (250);
      }
    }
    int size() {
      return write_i - read_i;
    }
    vector <DataT> get_as_vector() {
      return vector <DataT> (data + read_i, data + write_i);
    }
    void set_sink (bool new_sink) {
      if (new_sink == sink) {
        return;
      }
      sink = new_sink;
      if (sink) {
        bicycle_counter -= size();
      } else {
        bicycle_counter += size();
      }
    }
};

class TaskPull : public Node {
  private:
    DataStreamRaw <Task*> stream;
  public:
    inline void add_task (Task *task) {
      stream << task;
    }
    inline Task *get_task() {
      Maybe <Task *> x = stream.get();
      if (x.empty()) {
        return NULL;
      }
      return x;
    }

    inline void on_finish (void) {
    }
};

class EmptyStream {
  public:
    typedef EmptyStream FirstStreamType;
};

template <class FirstDataT>
class DataStream {
  private:
    DataStreamRaw <FirstDataT> *first_stream;
  public:
    typedef FirstDataT FirstDataType;
    typedef FirstDataT DataType;
    typedef DataStreamRaw <FirstDataT> FirstStreamType;
    typedef FirstStreamType StreamType;

    DataStream() : first_stream (NULL) {
    }
    virtual ~DataStream(){
    }

    DataStreamRaw <FirstDataT> *get_first_stream() {
      return first_stream;
    }
    void set_first_stream (DataStreamRaw <FirstDataT> *new_first_stream) {
      assert (first_stream == NULL);
      first_stream = new_first_stream;
    }
    DataStreamRaw <FirstDataT> *get_stream() {
      return get_first_stream();
    }
    void set_stream (DataStreamRaw <FirstDataT> *new_first_stream) {
      set_first_stream (new_first_stream);
    }

    void operator << (const FirstDataT &first) {
      *first_stream << first;
    }
};
template <class FirstDataT, class SecondDataT>
class DataStreamPair : public DataStream <FirstDataT> {
  private:
    typedef DataStream <FirstDataT> Base;
    DataStreamRaw <SecondDataT> *second_stream;
  public:
    typedef SecondDataT SecondDataType;
    typedef DataStreamRaw <SecondDataT> SecondStreamType;

    DataStreamPair() : second_stream (NULL) {
    }
    virtual ~DataStreamPair() {
    }

    DataStreamRaw <SecondDataT> *get_second_stream() {
      return second_stream;
    }
    void set_second_stream (DataStreamRaw <SecondDataT> *new_second_stream) {
      assert (second_stream == NULL);
      second_stream = new_second_stream;
    }

    using Base::operator <<;
    void operator << (const SecondDataT &second) {
      *second_stream << second;
    }
};

template <class FirstDataT, class SecondDataT, class ThirdDataT>
class DataStreamTriple : public DataStreamPair <FirstDataT, SecondDataT> {
  private:
    typedef DataStreamPair <FirstDataT, SecondDataT> Base;
    DataStreamRaw <ThirdDataT> *third_stream;
  public:
    typedef ThirdDataT ThirdDataType;
    typedef DataStreamRaw <ThirdDataT> ThirdStreamType;

    DataStreamTriple() : third_stream (NULL) {
    }
    virtual ~DataStreamTriple() {
    }

    DataStreamRaw <ThirdDataT> *get_third_stream() {
      return third_stream;
    }
    void set_third_stream (DataStreamRaw <ThirdDataT> *new_third_stream) {
      assert (third_stream == NULL);
      third_stream = new_third_stream;
    }

    using Base::operator <<;
    void operator << (const ThirdDataT &third) {
      *third_stream << third;
    }
};

template <class PipeType>
class PipeTask : public Task {
  private:
    typedef typename PipeType::InputType InputType;
    InputType input;
    PipeType *pipe_ptr;
  public:
    PipeTask (InputType input, PipeType *pipe_ptr) :
      input (input),
      pipe_ptr (pipe_ptr) {
      }
    void execute (void) {
      pipe_ptr->process_input (input);
    }
};

template <class PipeF, class InputStreamT, class OutputStreamT>
class Pipe : public Node {
  private:
    InputStreamT input_stream;
    OutputStreamT output_stream;
    PipeF function;
  public:
    typedef InputStreamT InputStreamType;
    typedef OutputStreamT OutputStreamType;

    typedef typename InputStreamT::DataType InputType;
    typedef Pipe <PipeF, InputStreamT, OutputStreamT> SelfType;
    typedef PipeTask <SelfType> TaskType;

    Pipe (bool parallel = true, bool sync_node = false) :
      Node (parallel, sync_node),
      input_stream(),
      output_stream() {
    }

    InputStreamT *get_input_stream() {
      return &input_stream;
    }
    OutputStreamT *get_output_stream() {
      return &output_stream;
    }

    void process_input (const InputType &input) {
      function.execute (input, *this);
    }

    Task *get_task() {
      Maybe <InputType> x = input_stream.get_stream()->get();
      if (x.empty()) {
        return NULL;
      }
      return new TaskType (x, this);
    }

    void on_finish() {
      function.on_finish (*this);
    }

    template <class ResultT>
      void operator << (const ResultT &result) {
        *get_output_stream() << result;
      }
};

template <class PipeT>
typename PipeT::InputStreamType &pipe_input (PipeT &pipe) {
  return *pipe.get_input_stream();
}

template <class PipeT>
typename PipeT::OutputStreamType &pipe_output (PipeT &pipe) {
  return *pipe.get_output_stream();
}

template <class StreamT>
class FirstStream {
  private:
    StreamT *ptr;
  public:
    typedef typename StreamT::FirstStreamType StreamType;
    FirstStream (StreamT &ptr) :
      ptr (&ptr) {
      }
    StreamType *get_stream() const {
      return ptr->get_first_stream();
    }
    void set_stream (StreamType *stream) const {
      ptr->set_first_stream (stream);
    }
};

template <class StreamT>
class SecondStream {
  private:
    StreamT *ptr;
  public:
    typedef typename StreamT::SecondStreamType StreamType;
    SecondStream (StreamT &ptr) :
      ptr (&ptr) {
      }
    StreamType *get_stream() const {
      return ptr->get_second_stream();
    }
    void set_stream (StreamType *stream) const {
      ptr->set_second_stream (stream);
    }
};

template <class StreamT>
class ThirdStream {
  private:
    StreamT *ptr;
  public:
    typedef typename StreamT::ThirdStreamType StreamType;
    ThirdStream (StreamT &ptr) :
      ptr (&ptr) {
      }
    StreamType *get_stream() const {
      return ptr->get_third_stream();
    }
    void set_stream (StreamType *stream) const {
      ptr->set_third_stream (stream);
    }
};

template <class StreamT>
FirstStream <StreamT> first_stream (StreamT &stream) {
  return FirstStream <StreamT> (stream);
}

template <class StreamT>
SecondStream <StreamT> second_stream (StreamT &stream) {
  return SecondStream <StreamT> (stream);
}

template <class StreamT>
ThirdStream <StreamT> third_stream (StreamT &stream) {
  return ThirdStream <StreamT> (stream);
}

template <class FirstT, class SecondT>
void connect (FirstT &first, SecondT &second) {
  typedef typename FirstT::StreamType StreamType;
  StreamType *first_stream = first.get_stream();
  StreamType *second_stream = second.get_stream();

  if (first_stream != NULL && second_stream != NULL) {
    assert (first_stream == second_stream);
    return;
  }
  if (first_stream != NULL) {
    second.set_stream (first_stream);
    return;
  }
  if (second_stream != NULL) {
    first.set_stream (second_stream);
    return;
  }

  StreamType *stream = new StreamType();
  first.set_stream (stream);
  second.set_stream (stream);
}
//TODO: it is horrible hack
template <class FirstT, class SecondT>
void connect (const FirstT &first, const SecondT &second) {
  FirstT new_first (first);
  SecondT new_second (second);
  connect (new_first, new_second);
}
template <class FirstT, class SecondT>
void connect (FirstT &first, const SecondT &second) {
  SecondT new_second (second);
  connect (first, new_second);
}

template <class FirstT, class SecondT>
void connect (const FirstT &first, SecondT &second) {
  FirstT new_first (first);
  connect (new_first, second);
}

typedef enum {
  scc_sync_node,
  scc_use_first_output,
  scc_use_second_output,
  scc_use_third_output
} SCCEnum;

template <SCCEnum Cmd> struct SCC {
};

inline SCC <scc_sync_node> sync_node() {
  return SCC <scc_sync_node>();
}
inline SCC <scc_use_first_output> use_first_output() {
  return SCC <scc_use_first_output>();
}
inline SCC <scc_use_second_output> use_second_output() {
  return SCC <scc_use_second_output>();
}
inline SCC <scc_use_third_output> use_third_output() {
  return SCC <scc_use_third_output>();
}

template <class SchedulerT, class PipeT, class PipeHolderT>
class SC_Pipe {
  private:
    typedef typename PipeT::OutputStreamType PtrType;
    typedef SC_Pipe <SchedulerT, PipeT, PipeHolderT> Self;

  public:
    SchedulerT *scheduler;
    PipeT *pipe;
    PipeHolderT pipe_holder;

    template <class OtherPipeHolderT>
      SC_Pipe (const SC_Pipe <SchedulerT, PipeT, OtherPipeHolderT> &other) :
        scheduler (other.scheduler),
        pipe (other.pipe),
        pipe_holder (pipe_output (*other.pipe)) {
        }

    SC_Pipe (SchedulerT *scheduler, PipeT *pipe, PipeHolderT pipe_holder) :
      scheduler (scheduler),
      pipe (pipe),
      pipe_holder (pipe_holder) {
        pipe->add_to_scheduler (scheduler);
      }

    Self &operator >> (SCC <scc_sync_node>) {
      scheduler->add_sync_node (pipe);
      return *this;
    }

    SC_Pipe <SchedulerT, PipeT, FirstStream <PtrType> > operator >> (SCC <scc_use_first_output>) {
      return *this;
    }
    SC_Pipe <SchedulerT, PipeT, SecondStream <PtrType> > operator >> (SCC <scc_use_second_output>) {
      return *this;
    }
    SC_Pipe <SchedulerT, PipeT, ThirdStream <PtrType> > operator >> (SCC <scc_use_third_output>) {
      return *this;
    }

    template <class NextPipeT>
    SC_Pipe <SchedulerT, NextPipeT, FirstStream <typename NextPipeT::OutputStreamType> > operator >> (NextPipeT &next_pipe) {
        connect (pipe_holder, pipe_input (next_pipe));
      return scheduler_constructor (*scheduler, next_pipe);
    }
};

template <class A, class B>
SC_Pipe <A, B, FirstStream <typename B::OutputStreamType> > scheduler_constructor (A &a, B &b) {
  return SC_Pipe <A, B, FirstStream <typename B::OutputStreamType> > (&a, &b, pipe_output (b));
}


/*** Multithreaded profiler ***/
#define TACT_SPEED (1e-6 / 2266.0)
class ProfilerRaw {
  private:
    long long count;
    long long ticks;
    size_t memory;
    int flag;
  public:
    void alloc_memory (size_t size) {
      count++;
      memory += size;
    }
    size_t get_memory() {
      return memory;
    }

    void start() {
      if (flag == 0) {
        ticks -= dl_rdtsc();
        count++;
      }
      flag++;
    }
    void finish() {
      //assert (flag == 1);
      flag--;
      if (flag == 0) {
        ticks += dl_rdtsc();
      }
    }
    long long get_ticks() {
      return ticks;
    }
    long long get_count() {
      return count;
    }
    double get_time() {
      return get_ticks() * TACT_SPEED;
    }
};


#define PROF_E_(x) prof_ ## x
#define PROF_E(x) PROF_E_(x)
#define FOREACH_PROF(F)\
  F (A)\
  F (B)\
  F (C)\
  F (D)\
  F (E)\
  F (next_name)\
  F (next_const_string_name)\
  F (create_function)\
  F (load_files)\
  F (lexer)\
  F (gentree)\
  F (apply_break_file)\
  F (split_switch)\
  F (collect_required)\
  F (calc_locations)\
  F (collect_defines)\
  F (register_defines)\
  F (preprocess_eq3)\
  F (preprocess_function_c)\
  F (preprocess_break)\
  F (register_variables)\
  F (calc_const_type)\
  F (collect_const_vars)\
  F (calc_throw_edges)\
  F (calc_throws)\
  F (check_function_calls)\
  F (calc_rl)\
  F (CFG)\
  F (type_infence)\
  F (tinf_infer)\
  F (tinf_infer_gen_dep)\
  F (tinf_infer_infer)\
  F (tinf_find_isset)\
  F (tinf_check)\
  F (CFG_End)\
  F (optimization)\
  F (calc_val_ref)\
  F (calc_func_dep)\
  F (calc_bad_vars)\
  F (check_ub)\
  F (final_check)\
  F (code_gen)\
  F (writer)\
  F (end_write)\
  \
  F (vertex_inner)\
  F (vertex_inner_data)\
  F (total)

#define DECLARE_PROF_E(x) PROF_E(x),
typedef enum {
  FOREACH_PROF (DECLARE_PROF_E)
  ProfilerId_size
} ProfilerId;

class Profiler {
  public:
    ProfilerRaw raw[ProfilerId_size];
    char dummy[4096];
};

extern TLS <Profiler> profiler;
inline ProfilerRaw &get_profiler (ProfilerId id) {
  return profiler->raw[id];
}

#define PROF(x) get_profiler (PROF_E (x))
inline void profiler_print (ProfilerId id, const char *desc) {
  double total_time = 0;
  long long total_ticks = 0;
  long long total_count = 0;
  size_t total_memory = 0;
  for (int i = 0; i <= MAX_THREADS_COUNT; i++) {
    total_time += profiler.get (i)->raw[id].get_time();
    total_count += profiler.get (i)->raw[id].get_count();
    total_ticks += profiler.get (i)->raw[id].get_ticks();
    total_memory += profiler.get (i)->raw[id].get_memory();
  }
  if (total_count > 0) {
    if (total_ticks > 0) {
      fprintf (
          stderr, "%40s:\t\%lf %lld %lld\n",
          desc, total_time, total_count, total_ticks / max (1ll, total_count)
      );
    }
    if (total_memory > 0) {
      fprintf (
          stderr, "%40s:\t\%.5lfMb %lld %.5lf\n",
          desc, (double)total_memory / (1 << 20), total_count, (double)total_memory / total_count
      );
    }
  }
}

inline void profiler_print_all() {
  #define PRINT_PROF(x) profiler_print (PROF_E(x), #x);
  FOREACH_PROF (PRINT_PROF);
}

template <ProfilerId Id>
class  AutoProfiler {
  private:
    ProfilerRaw &prof;
  public:
    AutoProfiler() :
      prof (get_profiler (Id)) {
      prof.start();
    }
    ~AutoProfiler() {
      prof.finish();
    }
};
#define AUTO_PROF(x) AutoProfiler <PROF_E (x)> x ## _auto_prof

/*** Multithreaded version of IdGen ***/
class BikeIdGen {
  private:
    int used_n;

    struct IdRange {
      int l, r;
    };
    TLS <IdRange> range;
  public:
    BikeIdGen() :
      used_n (0) {
    }
    int next_id() {
      IdRange &cur = *range;
      if (unlikely (cur.l == cur.r)) {
        int old_used_n;
        while (true) {
          old_used_n = used_n;
          if (__sync_bool_compare_and_swap (&used_n, old_used_n, old_used_n + 4096)) {
            break;
          }
          usleep (250);
        }
        cur.l = old_used_n;
        cur.r = old_used_n + 4096;
      }
      int index = cur.l++;
      return index;
    }
};

/*** Multithreaded hash table ***/
//long long -> T
//Too much memory, not resizable, do not support collisions. Yep.
static const int N = 1000000;
template <class T>
class HT {
  public:
    struct HTNode : Lockable {
      unsigned long long hash;
      T data;
      HTNode() :
        hash(0),
        data() {
      }
    };
  private:
    BikeIdGen id_gen;
    HTNode *nodes;
    int nodes_size;
  public:
    HT() :
      nodes (new HTNode[N]),
      nodes_size (N) {
    }
    HTNode *at (unsigned long long hash) {
      int i = (unsigned)hash % (unsigned)nodes_size;
      while (true) {
        while (nodes[i].hash != 0 && nodes[i].hash != hash) {
          i++;
          if (i == nodes_size) {
            i = 0;
          }
        }
        if (nodes[i].hash == 0 && !__sync_bool_compare_and_swap (&nodes[i].hash, 0, hash)) {
          int id = id_gen.next_id();
          assert (id * 2 < N);
          continue;
        }
        break;
      }
      return &nodes[i];
    }

    vector <T> get_all() {
      vector <T> res;
      for (int i = 0; i < N; i++) {
        if (nodes[i].hash != 0) {
          res.push_back (nodes[i].data);
        }
      }
      return res;
    }
};


#define BICYCLE_DL_PSTR
#define dl_pstr bicycle_dl_pstr
char* bicycle_dl_pstr (char const *message, ...) __attribute__ ((format (printf, 1, 2)));

