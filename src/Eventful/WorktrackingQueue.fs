﻿namespace Eventful

open System
open System.Collections.Generic

type internal CompleteQueueMessage<'TGroup, 'TItem when 'TGroup : comparison> = 
    | Start of 'TItem * Set<'TGroup> * Async<unit> * AsyncReplyChannel<unit>
    | Complete of 'TGroup * Guid
    | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type WorktrackingQueue<'TGroup, 'TItem when 'TGroup : comparison>
    (
        maxItems, 
        grouping : 'TItem -> Set<'TGroup>, 
        complete : 'TItem -> Async<unit>,
        workerCount,
        workAction : 'TGroup -> 'TItem seq -> Async<unit>
    ) =

    let queue = new GroupingBoundedQueue<'TGroup, ('TItem*Guid), unit>(maxItems)

    let agent = Agent.Start(fun agent ->

        let rec loop(state : WorktrackQueueState<'TGroup, AsyncReplyChannel<unit>>) = async {
         // Console.WriteLine(sprintf "State: %A" state)
         let! msg = agent.Receive()
         match msg with
         | Start (item, groups, complete, reply) -> 
            let itemKey = Guid.NewGuid()
            // Console.WriteLine(sprintf "Started %A" itemKey)
            return! async {
                for group in groups do
                    do! queue.AsyncAdd(group, (item,itemKey))
                reply.Reply()
                return! loop (state.Add(itemKey, groups, complete))
            }
         | Complete (group, itemKey) -> 
            // Console.WriteLine(sprintf "Completed %A" itemKey)
            let (completeCallback, completedBatchReplies, nextState) = state.ItemComplete(group, itemKey)
            for reply in completedBatchReplies do
                reply.Reply()

            match completeCallback with
            | Some reply -> do! reply
            | None -> ()

            return! loop(nextState)
         | NotifyWhenAllComplete reply ->
            let (batchCreated, nextState) = state.CreateBatch(reply)
            if(batchCreated) then
                return! loop(nextState)
            else 
                reply.Reply()
                return! loop(nextState)
        }
        loop WorktrackQueueState<_,_>.Empty
    )

    let doWork (group, items) = async {
         // Console.WriteLine(sprintf "Received %d items" (items |> List.length))
         do! workAction group (items |> List.map fst)
         for item in (items |> List.map snd) do
            // Console.WriteLine(sprintf "Posting completed %A" item)
            agent.Post (Complete (group, item))
    }

    let workers = 
        for i in [1..workerCount] do
            async {
                while true do
                    do! queue.AsyncConsume doWork
            } |> Async.Start

    member this.Add (item:'TItem) =
        async {
            let groups = grouping item
            do! agent.PostAndAsyncReply (fun ch -> Start (item, groups, complete item, ch))
        }
    member this.AsyncComplete () =
        async {
            do! agent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete ch)
        }

type WorktrackingQueueCs<'TGroup, 'TItem when 'TGroup : comparison>
    (
        maxItems, 
        grouping : Func<'TItem, Set<'TGroup>>, 
        complete : Func<'TItem', System.Threading.Tasks.Task>,
        workerCount,
        workAction :  Func<'TGroup, 'TItem seq, System.Threading.Tasks.Task>
    ) =
    let groupingfs = (fun i -> grouping.Invoke(i))
    let completeFs = (fun i -> async { 
                   let task = complete.Invoke(i) 
                   do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                   })
    let workActionFs = (fun g i -> async { 
                        let task = workAction.Invoke(g,i) 
                        do! task |> Async.AwaitIAsyncResult |> Async.Ignore
                    })
    let queue = new WorktrackingQueue<'TGroup, 'TItem> ( maxItems, groupingfs, completeFs, workerCount, workActionFs )

    member this.Add (item:'TItem) =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()
        
        async {
            do! queue.Add(item)
            tcs.SetResult(true)
        } |> Async.Start

        tcs.Task

    member this.AsyncComplete () =
        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        async {
            do! queue.AsyncComplete()
            tcs.SetResult(true)
        } |> Async.Start

        tcs.Task