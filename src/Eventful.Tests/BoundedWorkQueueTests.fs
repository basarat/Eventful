namespace Eventful.Tests

open Eventful
open System
open Xunit
open System.Threading.Tasks
open FsUnit.Xunit
#nowarn "40" // disable recursive definition warning
module BoundedWorkQueueTests = 

    [<Fact>]
    [<Trait("category", "performance")>]
    let ``Can enqueue and dequeue an item`` () : unit = 
        let rec boundedWorkQueue = new BoundedWorkQueue<int>(1000, workQueued)

        and workQueued item = async { boundedWorkQueue.WorkComplete(1) }

        for i in [0..10000000] do
            boundedWorkQueue.QueueWork i |> Async.RunSynchronously