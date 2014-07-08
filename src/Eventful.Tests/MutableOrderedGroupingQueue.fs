namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FSharpx.Collections

type GroupEntry<'TItem> = {
    Items : List<'TItem>
}
  
type MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem when 'TGroup : comparison> = 
  | AddItem of (seq<'TItem * 'TGroup> * Async<unit> * AsyncReplyChannel<unit>)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)

type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>(?maxItems) =

    //let log = Common.Logging.LogManager.GetCurrentClassLogger()
    let maxItems =
        match maxItems with
        | Some v -> v
        | None -> 10000
    
    // normal .NET dictionary for performance
    // very mutable
    let groupItems = new System.Collections.Generic.Dictionary<'TGroup, GroupEntry<'TItem>>()

    let addItemToGroup item group =
        let (exists, value) = groupItems.TryGetValue(group)
        let value = 
            if exists then value
            else { Items = List.empty } 
        let value' = { value with Items = item::value.Items }
        groupItems.Add(group, value')
        ()

    let dispatcherAgent = Agent.Start(fun agent -> 
        let rec empty () = 
            agent.Scan(fun msg -> 
            match msg with
            | AddItem x -> Some(enqueue x)
            | _ -> None)
        and hasWork () =
            agent.Scan(fun msg ->
            match msg with
            | AddItem x -> Some <| enqueue x
            | ConsumeWork x -> Some <| consume x)
        and enqueue (items, onComplete, reply) = async {
            System.Console.WriteLine "Enqueuing"
            for (item, group) in items do
                addItemToGroup item group
            reply.Reply()
            return! hasWork() }
        and consume (workCallback, reply) = async {
            System.Console.WriteLine "Consuming"
            let nextKey = groupItems.Keys |> Seq.head
            let values = groupItems.Item nextKey
            workCallback(nextKey,values.Items) |> Async.StartAsTask |> ignore
            reply.Reply()
            groupItems.Remove(nextKey) |> ignore
            if(groupItems.Count = 0) then
                return! empty ()
            else
                return! hasWork ()
        }
        empty () )

    member this.Add (input:'TInput, group: ('TInput -> (seq<'TItem * 'TGroup>)), ?onComplete : Async<unit>) =
        async {
            let items = group input
            let onCompleteCallback = async {
                match onComplete with
                | Some callback -> return! callback
                | None -> return ()
            }
            do! dispatcherAgent.PostAndAsyncReply(fun ch ->  AddItem (items, onCompleteCallback, ch))
            ()
        }

    member this.Consume (work:(('TGroup * seq<'TItem>) -> Async<unit>)) =
        async {
            do! dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))
        }

    member this.CurrentItemsComplete () = 
        async {
            do! Async.Sleep 1000
            return () // todo
        }

module MutableOrderedGroupingBoundedQueueTests = 
    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can process single item`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new Eventful.CounterAgent()
        let rec consumer (counter : Eventful.CounterAgent)  = async {
            do! queue.Consume((fun (g, items) -> async {
                do! counter.Incriment(items |> Seq.length)
                return ()
            }))
            return! consumer counter
        }

        consumer counter |> Async.StartAsTask |> ignore

        async {
            do! queue.Add(1, (fun _ -> Seq.singleton(1, 1)))
            do! queue.CurrentItemsComplete()
            let! result = counter.Get()
            Assert.Equal(1, result); 
        } |> Async.RunSynchronously