################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Queue.Backend.Fifo do
  @behaviour Exvalve.Queue
  require Logger
  use GenServer

  @moduledoc """
  The fifo backend provides a first in first out behaviour for valvex
  it uses a standard erlang queue - if this behaviour does not suit
  your application or your wish to use a custom queue feel free
  to create your own backend, be careful to ensure you adhere to
  the behaviour of a valvex queue and to include knowledge of
  tombstoning and locking to your backend
  """

  @doc """
  Consumes N number of items from the queue, in this backend N is determined
  by the poll_count parameter
  """
  @spec consume(Exvalve.exvalve_ref(),
    Exvalve.exvalve_ref(),
    Exvalve.queue_backend(),
    Exvalve.queue_key(),
    non_neg_integer(),
    non_neg_integer()) :: :ok
  def consume(exvalve, qpid, backend, key, timeout, poll_count) do
    do_consume(exvalve, qpid, backend, key, timeout, poll_count)
  end

  @doc """
  Begins a crossover; the replacement of the current queue settings
  with a different arrangment
  """
  @spec crossover( Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: :ok
  def crossover(q, nuq) do
    GenServer.cast(q, {:crossover, nuq})
    GenServer.cast(q, :restart_consumer)
  end

  @doc """
  Returns the queue state
  """
  @spec get_state(Exvalve.exvalve_ref()) :: Exvalve.Queue.queue_state()
  def get_state(q) do
    GenServer.call(q, :get_state)
  end

  @doc """
  Returns true if the consumer is active
  """
  @spec is_consuming(Exvalve.exvalve_ref()) :: true | false
  def is_consuming(q) do
    GenServer.call(q, :is_consuming)
  end

  @doc """
  Returns true if the queue is locked, more details about locking
  are in the readme
  """
  @spec is_locked(Exvalve.exvalve_ref()) :: true | false
  def is_locked(q) do
    GenServer.call(q, :is_locked)
  end

  @doc """
  Returns true if the queue is marked for deletion, it will
  only be deleted once empty and can only be guaranteed to be
  eventually empty if locked and currently being consumed
  """
  @spec is_tombstoned(Exvalve.exvalve_ref()) :: true | false
  def is_tombstoned(q) do
    GenServer.call(q, :is_tombstoned)
  end

  @doc """
  Lock the queue, refer to the readme for specifics as to what this means
  """
  @spec lock(Exvalve.exvalve_ref()) :: :ok
  def lock(q) do
    GenServer.cast(q, :lock)
  end

  @doc """
  pop the queue, in the case of this backend it's first in first out
  """
  @spec pop(Exvalve.exvalve_ref()) :: Exvalve.exvalve_queue_item()
  def pop(q) do
    GenServer.call(q, :pop)
  end

  @doc """
  pop_r is the reverse option of pop, in this the last in will be popped
  """
  @spec pop_r(Exvalve.exvalve_ref()) :: Exvalve.exvalve_queue_item()
  def pop_r(q) do
    GenServer.call(q, :pop_r)
  end

  @doc """
  Pushes a queue item to the queue. The item is placed at the end
  """
  @spec push(Exvalve.exvalve_ref(), Exvalve.exalve_queue_item()) :: :ok
  def push(q, value) do
    GenServer.cast(q, {:push, value})
  end

  @doc """
  Pushes a queue item to the queue. The item is placed at the front
  """
  @spec push_r(Exvalve.exvalve_ref(), Exvalve.exalve_queue_item()) :: :ok
  def push_r(q, value) do
    GenServer.cast(q, {:push_r, value})
  end

  @doc """
  Returns the current number of items in the queue
  """
  @spec size(Exvalve.exvalve_ref()) :: non_neg_integer()
  def size(q) do
    GenServer.call(q, :size)
  end

  @doc """
  Starts a timer that regularly calls the consume function
  to facilitate steady and constant slurping of the queue,
  how regularly is determined by the poll_rate. How many
  items per call is determined by the poll_count
  """
  @spec start_consumer(Exvalve.exvalve_ref()) :: :ok
  def start_consumer(q) do
    GenServer.cast(q, :start_consumer)
  end

  @doc """
  Starts a link with the queue gen_server
  """
  @spec start_link(Exvalve.exvalve_ref(), Exvalve.exvalve_queue()) :: Exvalve.exvalve_ref()
  def start_link(exvalve, {key, _, _, _, _, _, _} = q) do
    GenServer.start_link(__MODULE__, [exvalve, q], name: key)
  end

  @doc """
  Stops the regular consumption of the queue
  """
  @spec stop_consumer(Exvalve.exvalve_ref()) :: :ok
  def stop_consumer(q) do
    GenServer.cast(q, :stop_consumer)
  end

  @doc """
  Marks the queue for deletion
  """
  @spec tombstone(Exvalve.exvalve_ref()) :: :ok
  def tombstone(q) do
    GenServer.cast(q, :tombstone)
  end

  @doc """
  Unlocks the queue, for more information about what this means
  consult the readme
  """
  @spec unlock(Exvalve.exvalve_ref()) :: :ok
  def unlock(q) do
    GenServer.cast(q, :unlock)
  end

  ##==============================================================================
  ## GenServer callbacks
  ##==============================================================================
  def init([ exvalve, {key,
                       {:threshold, threshold},
                       {:timeout, timeout, :seconds},
                       {:pushback, pushback, :seconds},
                       {:poll_rate, poll_rate, :ms},
                       {:poll_count, poll_count},
                       backend
                      } = q]) do
    Exvalve.notify(exvalve, {:queue_started, q})
    {:ok, %Exvalve.Queue{:key => key,
                         :threshold => threshold,
                         :timeout => timeout,
                         :pushback => pushback,
                         :backend => backend,
                         :poll_rate => poll_rate,
                         :poll_count => poll_count,
                         :q => q,
                         :exvalve => exvalve
                        }}
  end

  def handle_call(:pop, _from, %Exvalve.Queue{ queue: q,
                                               q: rawq,
                                               size: size,
                                               tombstoned: tombstone,
                                               exvalve: exvalve } = s) do
    value = :queue.out(q)
    evaluate_value(exvalve, tombstone, size, rawq, value, :queue_popped, s)
  end
  def handle_call(:pop_r, _from, %Exvalve.Queue{ queue: q,
                                                 q: rawq,
                                                 size: size,
                                                 tombstoned: tombstone,
                                                 exvalve: exvalve } = s) do
    value = :queue.out_r(q)
    evaluate_value(exvalve, tombstone, size, rawq, value, :queue_popped_r, s)
  end
  def handle_call(:is_consuming, _from, %Exvalve.Queue{ consuming: consuming} = s) do
    {:reply, consuming, s}
  end
  def handle_call(:is_locked, _from, %Exvalve.Queue{locked: locked} = s) do
    {:reply, locked, s}
  end
  def handle_call(:is_tombstoned, _from, %Exvalve.Queue{tombstoned: tombstoned} = s) do
    {:reply, tombstoned, s}
  end
  def handle_call(:size, _from, %Exvalve.Queue{size: size} = s) do
    {:reply, size, s}
  end
  def handle_call(:get_state, _from, s) do
    {:reply, s, s}
  end

  def handle_cast({:push, {work, _timestamp} = value}, %Exvalve.Queue{ exvalve: exvalve,
                                                                       q: rawq,
                                                                       queue: q} = s) do
    Exvalve.notify(exvalve, {:queue_push, rawq})
    evaluate_push(s, :queue.in(value, q), work)
  end
  def handle_cast({:push_r, {work, _timestamp} = value}, %Exvalve.Queue{ exvalve: exvalve,
                                                                         q: rawq,
                                                                         queue: q} = s) do
    Exvalve.notify(exvalve, {:queue_push_r, rawq})
    evaluate_push(s, :queue.in_r(value, q), work)
  end
  def handle_cast(:lock, %Exvalve.Queue{ exvalve: exvalve,
                                         q: rawq} = s) do
    Exvalve.notify(exvalve, {:queue_locked, rawq})
    {:noreply, %{s | locked: true}}
  end
  def handle_cast(:unlock, %Exvalve.Queue{ exvalve: exvalve,
                                           q: rawq} = s) do
    Exvalve.notify(exvalve, {:queue_unlocked, rawq})
    {:noreply, %{s | locked: false}}
  end
  def handle_cast(:tombstone, %Exvalve.Queue{ exvalve: exvalve,
                                              q: rawq} = s) do
    Exvalve.notify(exvalve, {:queue_tombstoned, rawq})
    {:noreply, %{s | tombstoned: true}}
  end
  def handle_cast({:crossover, nuq}, %Exvalve.Queue{ key: key,
                                                     exvalve: exvalve,
                                                     q: rawq,
                                                   } = s) do
    Exvalve.notify(exvalve, {:queue_crossover, rawq, nuq})
    Exvalve.update(exvalve, key, nuq)
    do_crossover(nuq, s)
  end
  def handle_cast(:start_consumer, %Exvalve.Queue{ exvalve: exvalve,
                                                   queue_pid: qpid,
                                                   backend: backend,
                                                   key: key,
                                                   timeout: timeout,
                                                   poll_rate: poll_rate,
                                                   poll_count: poll_count,
                                                   q: rawq,
                                                   consumer: tref } = s) do
    if tref == nil do
      Logger.info("Starting consumer: #{inspect key}")
      start_timer(exvalve, qpid, backend, key, timeout, poll_rate, rawq, s, poll_count)
    else
      Logger.info("Consumer already started: #{inspect key}")
      {:noreply, s}
    end
  end
  def handle_cast(:stop_consumer, %Exvalve.Queue{ consumer: tref,
                                                  q: rawq,
                                                  exvalve: exvalve
                                                } = s) do
    if tref do
      :timer.cancel(tref)
      Exvalve.notify(exvalve, {:queue_consumer_stopped, rawq})
    end
    {:noreply, %{s | consumer: nil, consuming: false}}
  end

  ##==============================================================================
  ## Helpers
  ##==============================================================================
  defp do_consume(_exvalve, _qpid, _backend, _key, _timeout, -1) do
    :ok
  end
  defp do_consume(exvalve, qpid, backend, key, timeout, n) do
    queue_value = GenServer.call(qpid, :pop)
    case queue_value do
      {{:value, {work, timestamp}}, _q} ->
        case is_stale(timeout, timestamp) do
          false ->
            GenServer.call(exvalve, {:assign_work, {work, timestamp}, {key, qpid, backend}})
          true ->
            Exvalve.notify(exvalve, {:timeout, key})
        end
      {:empty, :tombstoned} ->
        Exvalve.notify(exvalve, {:queue_removed, key})
        Exvalve.remove(exvalve, key, :force_remove)
      {:empty, _} ->
        :ok
    end
    do_consume(exvalve, qpid, backend, key, timeout, n-1)
  end

  defp update_state(q, size, s) do
    %{s | size: size, queue: q}
  end

  defp evaluate_value(exvalve, tombstone, size, rawq, value, event, s) do
    case value do
      {{:value, {_work, _timestamp}}, q} ->
        Exvalve.notify(exvalve, {event, rawq})
        {:reply, value, update_state(q, size-1, s)}
      {:empty, _} ->
        case tombstone do
          false -> {:reply, value, s}
          true  -> {:reply, {:empty, :tombstoned}, s}
        end
    end
  end

  defp evaluate_push(%Exvalve.Queue{ key: key,
                                     exvalve: exvalve,
                                     q: rawq,
                                     threshold: threshold,
                                     size: size,
                                     locked: locked
                                   } = s, q, work) do
    case locked do
      false ->
        case size >= threshold do
          true ->
            Exvalve.pushback(exvalve, key)
            {:noreply, s}
          false ->
            Logger.info("Work pushed, Key: #{inspect key}, Value: #{inspect work}")
            Exvalve.notify(exvalve, {:push_complete, rawq})
            {:noreply, update_state(q, size+1, s)}
        end
      true ->
        Logger.warn("Push to a locked queue: #{inspect key}")
        Exvalve.notify(exvalve, {:push_to_locked_queue, key})
        {:noreply, s}
    end
  end

  defp do_crossover({ _key,
                      {:threshold, threshold},
                      {:timeout, timeout, :seconds},
                      {:pushback, pushback, :seconds},
                      {:poll_rate, poll_rate, :ms},
                      {:poll_count, poll_count},
                      _backend
                    } = q, s) do
    {:noreply, %{s |
                 threshold: threshold,
                 timeout: timeout,
                 pushback: pushback,
                 poll_rate: poll_rate,
                 poll_count: poll_count,
                 q: q}}
  end

  defp start_timer(exvalve, qpid, backend, key, timeout, poll, rawq, s, poll_count) do
    {:ok, tref} = :timer.apply_interval(
      poll,
      __MODULE__,
      :consume,
      [exvalve, qpid, backend, key, timeout, poll_count]
    )
    Exvalve.notify(exvalve, {:queue_consumer_started, rawq})
    {:noreply, %{s | consumer: tref, consuming: true}}
  end

  defp is_stale(timeout, timestamp) do
    timeoutms = :timer.seconds(timeout)
    diff = :timer.now_diff(:erlang.timestamp(), timestamp)
    diff > (timeoutms * 1000)
  end
end
