################################################################################
### @author Howard Beard-Marlowe <howardbm@live.se>
### @copyright 2016 Howard Beard-Marlowe
### @version 1.0.2
################################################################################
defmodule Exvalve.Server do
  require Logger
  use GenServer

  defstruct queues: [],
    queue_pids: [],
    pushback: false,
    workers: [],
    event_server: nil,
    available_workers: []

  @moduledoc """
  The server is responsible for queue management, event sending and pushback
  """

  @typedoc """
  The internal state kept by exvalve_server
  """
  @type exvalve_state :: %{ :queues            => [Exvalve.exvalve_queue()],
                            :queue_pids        => [{atom(), module()}],
                            :pushback          => true | false,
                            :workers           => Exvalve.exvalve_workers(),
                            :event_server      => Exvalve.exvalve_ref() | nil,
                            :available_workers => Exvalve.exvalve_workers() }

  ##==============================================================================
  ## API Functions
  ##==============================================================================
  @doc """
  Starts the exvalve server
  """
  @spec start_link(Exvalve.exvalve_options()) :: Exvalve.exvalve_ref()
  def start_link(options) do
    {:ok, pid} = GenServer.start_link(__MODULE__, options, name: :exvalve)
    pid
  end

  @doc """
  Adds a queue to exvalve, there are some alternative options
  that can be used to alter the behaviour of add.
  """
  @spec add(Exvalve.exvalve_ref(), Exvalve.exvalve_queue(), Exvalve.add_option()) :: :ok | Exvalve.unique_key_error()
  def add(exvalve, { _key,
                     {:threshold, _threshold},
                     {:timeout, _timeout, :seconds},
                     {:pushback, _pushback, :seconds},
                     {:poll_rate, _poll, :ms},
                     {:poll_count, _pollCount},
                     _backend
                   } = q, option) do
    do_add(exvalve, q, option)
  end

  @doc """
  Removes a queue from valvex, there are some alternative options
  that can be used to alter the behaviour of remove.
  """
  @spec remove(Exvalve.exvalve_ref(), Exvalve.queue_key(), Exvalve.remove_option()) :: :ok | Exvalve.key_find_error()
  def remove(exvalve, key, option) do
    do_remove(exvalve, key, option)
  end

  @doc """
  Pushes an item to a queue with the given key
  """
  @spec push(Exvalve.exvalve_ref(), Exvalve.queue_key(), Exvalve.exvalve_queue_item()) :: :ok
  def push(exvalve, key, value) do
    do_push(exvalve, key, value)
  end

  @doc """
  Gets available workers
  """
  @spec get_available_workers(Exvalve.exvalve_ref()) :: Exvalve.exvalve_workers()
  def get_available_workers(exvalve) do
    do_get_workers(exvalve)
  end

  @doc """
  Gets the current size of the queue with the given key
  """
  @spec get_queue_size(Exvalve.exvalve_ref(), Exvalve.queue_key()) :: non_neg_integer() | Exvalve.key_find_error()
  def get_queue_size(exvalve, key) do
    do_get_queue_size(exvalve, key)
  end

  @doc """
  Pushes back on the client forcing them to wait
  this prevents spamming if enabled
  """
  @spec pushback(Exvalve.exvalve_ref(), Exvalve.queue_key()) :: :ok
  def pushback(exvalve, key) do
    do_pushback(exvalve, key)
  end

  @doc """
  Notifies all registered event handlers of an event
  within valvex
  """
  @spec notify(Exvalve.exvalve_ref(), any()) :: :ok
  def notify(exvalve, event) do
    do_notify(exvalve, event)
  end

  @doc """
  Adds a handler which will receive events from exvalve,
  view the readme for more info on events
  """
  @spec add_handler(Exvalve.exvalve_ref(), module(), list()) :: :ok
  def add_handler(exvalve, mod, args) do
    do_add_handler(exvalve, mod, args)
  end

  @doc """
  Removes a previously added handler
  """
  @spec remove_handler(Exvalve.exvalve_ref(), module(), list()) :: :ok
  def remove_handler(exvalve, mod, args) do
    do_remove_handler(exvalve, mod, args)
  end

  @doc """
  Updates a queue after a crossover
  """
  @spec update(Exvalve.exvalve_ref(), Exvalve.queue_key(), Exvalve.exvalve_queue()) :: :ok
  def update(exvalve, key, nuq) do
    do_update(exvalve, key, nuq)
  end

  ##==============================================================================
  ## GenServer Callbacks
  ##==============================================================================
  @doc """
  Initializes valvex with predefined queues, settings and event_handlers
  """
  @spec init([tuple()]) :: {:ok, exvalve_state()}
  def init([ {:queues, queues},
             {:pushback_enabled, pushback},
             {:workers, worker_count},
             {:event_handlers, event_handlers}
           ]) do
    Process.flag(:trap_exit, true)
    GenServer.cast(self(), {:init, queues, pushback, worker_count, event_handlers})
    {:ok, %Exvalve.Server{}}
  end
  @doc """
  Initializes valvex with default settings.
  """
  def init([]) do
    Process.flag(:trap_exit, true)
    workers = do_start_workers(10)
    {:ok, eventserver} = GenEvent.start_link()
    {:ok, %Exvalve.Server{ :queues => [],
                           :queue_pids => [],
                           :pushback => false,
                           :workers => workers,
                           :event_server => eventserver,
                           :available_workers => workers
                         }}
  end

  def handle_call({:get_queue, key}, _from, s = %Exvalve.Server{ queue_pids: queues }) do
    case :lists.keyfind(key, 1, queues) do
      false ->
        {:reply, {:error, :key_not_found}, s}
      {_key, _backend} = queue ->
        Logger.info("Get queue found: #{inspect queue}")
        {:reply, queue, s}
    end
  end
  def handle_call({:get_raw_queue, key}, _from, s = %Exvalve.Server{ queues: raw_queues }) do
    case :lists.keyfind(key, 1, raw_queues) do
      false ->
        {:reply, {:error, :key_not_found}, s}
      rawq ->
        Logger.info("get_raw_queue found: #{rawq}")
        {:reply, rawq, s}
    end
  end
  def handle_call({:add, {key,
                          {:threshold, _},
                          {:timeout, _, :seconds},
                          {:pushback, _, :seconds},
                          {:poll_rate, _, :ms},
                          {:poll_count, _},
                          backend
                         } = q, option}, _from, s = %Exvalve.Server{ queues: qs,
                                                                     queue_pids: qpids
                                                                   }) do
    newqueues = :lists.append(qs, [q])
    Exvalve.Queue.Supervisor.start_child([backend, key, q])
    unless option == :manual_start do
      Exvalve.Queue.start_consumer(backend, key)
    end
    newqpids = :lists.append(qpids, [{key, backend}])
    Logger.info("queue added: #{inspect q}")
    {:reply, :ok, %{s | queues: newqueues, queue_pids: newqpids}}
  end

  def handle_call(:get_workers, _from, %Exvalve.Server{ available_workers: workers } = s) do
    Logger.info("List of workers requested: #{inspect workers}")
    {:reply, workers, s}
  end
  def handle_call({:assign_work, {work, timestamp}, {key, qpid, backend}}, _from, %Exvalve.Server{available_workers: workers} = s) do
    case workers == [] do
      false ->
        [worker | t] = workers
        exvalve = self()
        workfun = fn() -> Exvalve.notify(exvalve, {:result, work.(), key}) end
        Exvalve.notify(exvalve, {:worker_assigned, key, worker, t})
        Exvalve.Worker.do_work(worker, {:work, workfun})
        {:noreply, %{s | available_workers: t}}
      true ->
        Exvalve.notify(self(), {:work_requeued, key, []})
        Exvalve.Queue.push_r(backend, qpid, {work, timestamp})
        {:noreply, s}
    end
  end
  def handle_call({:remove, key}, _from, %Exvalve.Server{ queues: queues,
                                                          queue_pids: qpids
                                                        } = s) do
    Supervisor.terminate_child(Exvalve.Queue.Supervisor, key)
    Supervisor.delete_child(Exvalve.Queue.Supervisor, key)
    {:reply, :ok, %{s | queues: :lists.keydelete(key, 1, queues),
                    queue_pids: :lists.keydelete(key,1, qpids)}}
  end
  def handle_call({:add_handler, module, args}, _from, %Exvalve.Server{event_server: event_server} = s) do
    Logger.info("Handler added, Module: #{inspect module}, Args: #{inspect args}")
    :ok = GenEvent.add_handler(event_server, module, args)
    {:reply, :ok, s}
  end
  def handle_call({:remove_handler, module, args}, _from, %Exvalve.Server{event_server: event_server} = s) do
    Logger.info("Handler removed, Module: #{inspect module}, Args: #{inspect args}")
    :ok = GenEvent.remove_handler(event_server, module, args)
    {:reply, :ok, s}
  end
  def handle_call({:update, key, { key,
                                   _,
                                   _,
                                   _,
                                   _,
                                   _,
                                   backend} = q}, _from, %Exvalve.Server{ queues: queues,
                                                                          queue_pids: qpids
                                                                        } = s) do
    Logger.info("Queue options updated, Key: #{inspect key}, Update: #{inspect q}")
    nuqs = :lists.append(:lists.keydelete(key, 1, queues), [q])
    nuqpids = :lists.append(:lists.keydelete(key, 1, qpids), [{key, backend}])
    {:reply, :ok, %{s | queues: nuqs,
                    queue_pids: nuqpids}}
  end
  def handle_call(:get_queues, _from, %Exvalve.Server{queues: queues} = s) do
    {:reply, queues, s}
  end

  def handle_cast({:init, queues, pushback, worker_count, event_handlers}, s) do
    {:ok, event_server} = GenEvent.start_link()
    event_handlers |>
      Enum.each(fn({event_module, args}) -> GenEvent.add_handler(event_server, event_module, args) end)
    qpids = queues |> Enum.flat_map(fn({key, _, _, _, _, _, backend} = q) ->
      Exvalve.Queue.Supervisor.start_child([backend, key, q])
      Exvalve.Queue.start_consumer(backend, key)
      [{key, backend}]
    end)
    workers = do_start_workers(worker_count)
    {:noreply, %Exvalve.Server{ s | queues: queues,
                                queue_pids: qpids,
                                pushback: pushback,
                                workers: workers,
                                event_server: event_server,
                                available_workers: workers
                              }}
  end
  def handle_cast({:pushback, key}, %Exvalve.Server{queues: queues} = s) do
    case :lists.keyfind(key, 1, queues) do
      {^key, _, _, {:pushback, pushback, :seconds}, _, _, _} = q ->
        exvalve = self()
        :erlang.spawn(fn() ->
          timeoutms = :timer.seconds(pushback)
          :timer.sleep(timeoutms)
          notify(exvalve, {:threshold_hit, q})
        end)
        {:noreply, s}
      false ->
        {:noreply, s}
    end
  end
  def handle_cast({:notify, event}, %Exvalve.Server{ event_server: event_server} = s) do
    GenEvent.notify(event_server, event)
    {:noreply, s}
  end

  def terminate(_reason, %Exvalve.Server{ workers: workers, event_server: event_server} = _s) do
    workers |> Enum.each(fn(worker) -> Exvalve.Worker.stop(worker) end)
    GenEvent.stop(event_server)
  end
  ##==============================================================================
  ## Private
  ##==============================================================================

  defp do_get_queue(exvalve, key) do
    GenServer.call(exvalve, {:get_queue, key})
  end

  defp do_add(exvalve, q, option, :skip_get) do
    GenServer.call(exvalve, {:add, q, option})
  end

  defp do_add(exvalve, {key, _, _, _, _, _, _} = q, nil) do
    case do_get_queue(exvalve, key) do
      {:error, :key_not_found} ->
        Logger.info("Running GenServer call {:add, #{inspect q}, nil}")
        GenServer.call(exvalve, {:add, q, nil})
      _ ->
        Logger.error("Attempted to add a non-unique key: #{key}")
        {:error, :key_not_unique}
    end
  end
  defp do_add(exvalve, {key, _, _, _, _, _, backend} = q, :crossover_on_existing) do
    case do_get_queue(exvalve, key) do
      {:error, :key_not_found} ->
        do_add(exvalve, q, nil, :skip_get)
      {^key, ^backend} ->
        Exvalve.Queue.crossover(backend, key, q)
      {_, _} ->
        Logger.error("Attempted to switch backend/key - operation not supported")
        {:error, :backend_key_crossover_not_supported}
    end
  end
  defp do_add(exvalve, {key, _, _, _, _, _, _} = q, :manual_start) do
    case do_get_queue(exvalve, key) do
      {:error, :key_not_found} ->
        do_add(exvalve, q, :manual_start, :skip_get)
      {key, _} ->
        Logger.error("Attempted to add a non-unique key: #{key}")
        {:error, :key_not_unique}
    end
  end

  defp do_remove(exvalve, key, nil) do
    case do_get_queue(exvalve, key) do
      {^key, backend} ->
        Exvalve.Queue.tombstone(backend, key)
      {:error, :key_not_found} = error ->
        Logger.error("Attempted to remove a non-existing key: #{key}")
        error
    end
  end
  defp do_remove(exvalve, key, :lock_queue) do
    case do_get_queue(exvalve, key) do
      {^key, backend} ->
        Exvalve.Queue.lock(backend, key)
        Exvalve.Queue.tombstone(backend, key)
      {:error, :key_not_found} = error ->
        Logger.error("Attempted to remove a non-existing key: #{key}")
        error
    end
  end
  defp do_remove(exvalve, key, :force_remove) do
    case do_get_queue(exvalve, key) do
      {^key, _backend} ->
        GenServer.call(exvalve, {:remove, key})
      {:error, :key_not_found} = error ->
        Logger.error("Attempted to remove a non-existing key: #{key}")
        error
    end
  end

  defp do_push(exvalve, key, value) do
    GenServer.cast(exvalve, {:push, key, value})
  end

  defp do_get_workers(exvalve) do
    GenServer.call(exvalve, :get_workers)
  end

  defp do_get_queue_size(exvalve, key) do
    case do_get_queue(exvalve, key) do
      {key, backend} ->
        Exvalve.Queue.size(backend, key)
      error ->
        Logger.error("Attempted to get the size of a non-existing key: #{key}")
        error
    end
  end

  defp do_pushback(exvalve, key) do
    GenServer.cast(exvalve, {:pushback, key})
  end

  defp do_notify(exvalve, event) do
    GenServer.cast(exvalve, {:notify, event})
  end

  defp do_add_handler(exvalve, mod, args) do
    GenServer.call(exvalve, {:add_handler, mod, args})
  end

  defp do_remove_handler(exvalve, mod, args) do
    GenServer.call(exvalve, {:remove_handler, mod, args})
  end

  defp do_update(exvalve, key, {key, _, _, _, _, _, backend} = q) do
    case do_get_queue(exvalve, key) do
      {:error, :key_not_found} = error ->
        Logger.error("Attempted to update a non-existing queue: #{inspect q}")
        error
      {^key, ^backend} ->
        GenServer.call(exvalve, {:update, key, q})
      _ ->
        Logger.error("Attempted to switch backend - operation not supported")
        {:error, :backend_key_crossover_not_supported}
    end
  end
  defp do_update(_exvalve, _key, {_otherkey, _, _, _, _, _, _}) do
    Logger.error("Attempted to switch key - operation not supported")
    {:error, :backend_key_crossover_not_supported}
  end

  defp do_start_workers(worker_count) do
    1..worker_count  |>
      Enum.map(fn(_) -> [Exvalve.Worker.start_link(self())] end)
  end
end
