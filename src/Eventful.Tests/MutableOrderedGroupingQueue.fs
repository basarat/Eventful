namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FSharpx.Collections

type GroupEntry<'TItem> = {
    Items : List<Int64 * 'TItem>
}
  
type MutableOrderedGroupingBoundedQueueMessages<'TGroup, 'TItem when 'TGroup : comparison> = 
  | AddItem of (seq<'TItem * 'TGroup> * Async<unit> * AsyncReplyChannel<unit>)
  | ConsumeWork of (('TGroup * seq<'TItem> -> Async<unit>) * AsyncReplyChannel<unit>)
  | NotifyWhenAllComplete of AsyncReplyChannel<unit>

type MutableOrderedGroupingBoundedQueue<'TGroup, 'TItem when 'TGroup : comparison>(?maxItems) =

    //let log = Common.Logging.LogManager.GetCurrentClassLogger()
    let maxItems =
        match maxItems with
        | Some v -> v
        | None -> 10000
    
    // normal .NET dictionary for performance
    // very mutable
    let groupItems = new System.Collections.Generic.Dictionary<'TGroup, GroupEntry<'TItem>>()

    let lastCompleteTracker = new LastCompleteItemAgent2<int64>()

    let addItemToGroup item group =
        let (exists, value) = groupItems.TryGetValue(group)
        let value = 
            if exists then value
            else { Items = List.empty } 
        let value' = { value with Items = item::value.Items }
        groupItems.Remove group |> ignore
        groupItems.Add(group, value')
        ()

    let dispatcherAgent = Agent.Start(fun agent -> 
        let rec empty itemIndex = 
            agent.Scan(fun msg -> 
            match msg with
            | AddItem x -> Some (enqueue x itemIndex)
            | NotifyWhenAllComplete reply -> 
                lastCompleteTracker.NotifyWhenComplete(itemIndex, async { reply.Reply() } )
                Some(empty itemIndex)
            | _ -> None)
        and hasWork itemIndex =
            agent.Scan(fun msg ->
            match msg with
            | AddItem x -> Some <| enqueue x itemIndex
            | ConsumeWork x -> Some <| consume x itemIndex
            | NotifyWhenAllComplete reply ->
                lastCompleteTracker.NotifyWhenComplete(itemIndex, async { reply.Reply() } )
                Some(hasWork itemIndex))
        and enqueue (items, onComplete, reply) itemIndex = async {
            // todo no good reason for this to be mutable
            let mutable index' = itemIndex
            let indexedItems = Seq.zip items (Seq.initInfinite (fun x -> itemIndex + int64 x))
            for ((item, group), index) in indexedItems do
                addItemToGroup (index, item) group
                do! lastCompleteTracker.Start index
            reply.Reply()
            // todo watch out for no items
            let nextIndex = indexedItems |> Seq.map snd |> Seq.max
            return! hasWork (nextIndex) }
        and consume (workCallback, reply) itemIndex = async {
            let nextKey = groupItems.Keys |> Seq.head
            let values = groupItems.Item nextKey
            async {
                try
                    do! workCallback(nextKey,values.Items |> List.rev |> List.map snd) 
                with | e ->
                    System.Console.WriteLine ("Error" + e.Message)
                
                for (i, _) in values.Items do
                    lastCompleteTracker.Complete i

            } |> Async.StartAsTask |> ignore

            reply.Reply()
            groupItems.Remove(nextKey) |> ignore
            if(groupItems.Count = 0) then
                return! empty itemIndex
            else
                return! hasWork itemIndex
        }
        empty 0L )

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
        dispatcherAgent.PostAndAsyncReply(fun ch -> ConsumeWork(work, ch))

    member this.CurrentItemsComplete () = 
        dispatcherAgent.PostAndAsyncReply(fun ch -> NotifyWhenAllComplete(ch))

module MutableOrderedGroupingBoundedQueueTests = 
    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can process single item`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<int, int>()
        let counter = new Eventful.CounterAgent()
        let rec consumer (counter : Eventful.CounterAgent)  = async {
            Console.WriteLine "Consuming single value"

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

    open System.Collections.Generic

    [<Fact>]
    [<Trait("category", "foo5")>]
    let ``speed test for 1 million items to tracker`` () : unit =
        let maxValue  = 1000000L
        let items = [1L..maxValue]
        let rnd = new Random(1024)
        let randomItems = items |> Seq.sortBy (fun _ -> rnd.Next(1000000)) |> Seq.cache

        let tracker = new LastCompleteItemAgent2<int64>()

        let tcs = new System.Threading.Tasks.TaskCompletionSource<bool>()

        async {
            for item in items do
                do! tracker.Start(item)

            for item in randomItems do
                tracker.Complete(item)
            
            tracker.NotifyWhenComplete (maxValue, async { tcs.SetResult true })
        } |> Async.RunSynchronously

        tcs.Task.Wait()

    [<Fact>]
    [<Trait("category", "foo5")>]
    let ``speed test for 1 million items to empty agent`` () : unit =
        let maxValue  = 1000000L
        let items = [1L..maxValue]
        let rnd = new Random(1024)
        let randomItems = items |> Seq.sortBy (fun _ -> rnd.Next(1000000)) |> Seq.cache

        let agent = Agent.Start(fun agent -> 
            let rec loop () = async {
                let! (msg : AsyncReplyChannel<unit>) = agent.Receive()
                msg.Reply() 
                return! loop ()}
            loop ()) 

        async {
            for item in items do
                do! agent.PostAndAsyncReply(fun ch -> ch)

            for item in randomItems do
                do! agent.PostAndAsyncReply(fun ch -> ch)
            
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("category", "foo3")>]
    let ``Can calculate correct values`` () : unit = 
        let queue = new MutableOrderedGroupingBoundedQueue<Guid, int>()
        let store = new System.Collections.Generic.Dictionary<Guid, int>()
        let monitor = new Object()
        let items = TestEventStream.sequentialValues 10000 100

        let accumulator s a =
            if(a % 2 = 0) then
                s + a
            else
                s - a

        let rec consumer ()  = async {
            do! queue.Consume((fun (g, items) -> async {
                // Console.WriteLine("{0} count: {1}", g, items |> Seq.length)
                lock monitor (fun () -> 
                    let current = 
                        if store.ContainsKey g then
                            store.Item(g)
                        else 
                            0
                    let result = 
                        items |> Seq.fold accumulator current
                   
                    store.Remove g |> ignore

                    store.Add(g, result)
                    ()
                )
                return ()
            }))
            return! consumer ()
        }

        consumer () |> Async.StartAsTask |> ignore

        async {
            for (eventPosition, key, value) in items do
                do! queue.Add(value, (fun v -> Seq.singleton (value, key)))
            do! queue.CurrentItemsComplete()
            Assert.Equal(10000, store.Count)
            for pair in store do
                Assert.Equal(50, pair.Value)
        } |> Async.RunSynchronously