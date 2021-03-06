﻿namespace Eventful.Tests.Integration

open Xunit
open System
open EventStore.ClientAPI
open FsUnit.Xunit
open Eventful
open Eventful.EventStream
open Eventful.EventStore
open Eventful.Testing

open FSharpx.Option

module EventStoreStreamInterpreterTests = 

    type MyEvent = {
        Name : string
    }

    let newId () : string =
        System.Guid.NewGuid().ToString()

    let event = { Name = "Andrew Browne" }
    let metadata = { SourceMessageId = System.Guid.NewGuid().ToString(); MessageId = System.Guid.NewGuid() }

    let eventNameMapping = 
        Bimap.Empty
        |> Bimap.addNew typeof<MyEvent>.Name (new ComparableType(typeof<MyEvent>))

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Write and read Event`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer eventNameMapping program

            let stream = "MyStream-" + (newId())

            let! writeResult = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes : seq<EventStreamEvent<TestMetadata>> = Seq.singleton eventToWrite
                    let! ignore = writeToStream stream NewStream writes
                    return "Write Complete"
                } |> run

            writeResult |> should equal "Write Complete"

            let! readResult =
                eventStream {
                    let! item = readFromStream stream EventStore.ClientAPI.StreamPosition.Start
                    return!
                        match item with
                        | Some x -> 
                            eventStream { 
                                let! (objValue, _) = readValue x
                                let value = objValue :?> MyEvent
                                return Some value.Name
                            }
                        | None -> eventStream { return None } 
                } |> run

            readResult |> should equal (Some "Andrew Browne")

        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Wrong Expected Version is Returned`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer eventNameMapping program

            let stream = "MyStream-" + (newId())

            let wrongExpectedVersion = AggregateVersion 10
            let! writeResult = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes : seq<EventStreamEvent<TestMetadata>> = Seq.singleton eventToWrite
                    return! writeToStream stream wrongExpectedVersion writes
                } |> run

            writeResult |> should equal WrongExpectedVersion
        } |> Async.RunSynchronously

    [<Fact>]
    [<Trait("requires", "eventstore")>]
    let ``Create a link`` () : unit =
        async {
            let! connection = RunningTests.getConnection()
            let client = new Client(connection)

            do! client.Connect()

            let run program =
                EventStreamInterpreter.interpret client RunningTests.esSerializer eventNameMapping program

            let sourceStream = "SourceStream-" + (newId())
            let stream = "MyStream-" + (newId())

            let! writeLink = 
                eventStream {
                    let! eventToWrite = getEventStreamEvent event metadata
                    let writes = Seq.singleton eventToWrite
                    let! ignore = writeToStream sourceStream NewStream writes
                    let! ignore = EventStream.writeLink stream NewStream sourceStream 0 metadata
                    return ()
                } |> run

            let! readResult =
                eventStream {
                    let! item = readFromStream stream EventStore.ClientAPI.StreamPosition.Start
                    return!
                        match item with
                        | Some x -> 
                            eventStream { 
                                let! (objValue,_) = readValue x
                                let value = objValue :?> MyEvent
                                return Some value.Name
                            }
                        | None -> eventStream { return None } 
                } |> run

            readResult |> should equal (Some event.Name)
        } |> Async.RunSynchronously