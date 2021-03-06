﻿namespace Eventful.Testing

open Xunit
open FsUnit.Xunit
open System
open Eventful
open Eventful.EventStream
open FSharpx.Collections

module EventStreamStateBuilder = 

    type WidgetAddedEvent = {
        Name : string
    }

    let stateBuilder =
        StateBuilder.Empty List.empty
        |> StateBuilder.addHandler (fun s (e:WidgetAddedEvent) -> e.Name::s)

    let runProgram eventStoreState p = 
        TestInterpreter.interpret p eventStoreState Bimap.Empty Map.empty Vector.empty |> snd

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from single event`` () : unit =
        let newMetadata () =
            { 
                MessageId = (Guid.NewGuid()) 
                SourceMessageId = (Guid.NewGuid().ToString())
            }

        let streamName = "TestStream-1"
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1" }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> StateBuilder.toStreamProgram streamName
        let result = runProgram eventStoreState program

        result |> should equal (1, Some ["Widget1"])

        ()

    [<Fact>]
    [<Trait("category", "unit")>]
    let ``Can build state from multiple events`` () : unit =
        let newMetadata () =
            { 
                MessageId = (Guid.NewGuid()) 
                SourceMessageId = (Guid.NewGuid().ToString())
            }

        let streamName = "TestStream-1"
 
        let eventStoreState = 
            TestEventStore.empty       
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget1" }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})
            |> TestEventStore.addEvent streamName (Event { Body = { Name = "Widget2" }; EventType =  "WidgetAddedEvent"; Metadata = newMetadata()})

        let program = stateBuilder |> StateBuilder.toStreamProgram streamName

        let result = runProgram eventStoreState program

        result |> should equal (2, Some ["Widget2";"Widget1"])

        ()