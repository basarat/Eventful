﻿namespace EmergencyRoom

open System 
open FSharpx

open Eventful
open Eventful.Aggregate
open Eventful.AggregateActionBuilder

type AggregateType =
| Visit
| Patient

type EmergencyEventMetadata = {
    MessageId: Guid
    SourceMessageId: string
    EventTime : DateTime
}

module Common = 
    let systemConfiguration = { 
        SetSourceMessageId = (fun id metadata -> { metadata with SourceMessageId = id })
        SetMessageId = (fun id metadata -> { metadata with MessageId = id })
    }

    let stateBuilder = NamedStateBuilder.nullStateBuilder<EmergencyEventMetadata>

    let emptyMetadata () = { SourceMessageId = String.Empty; MessageId = Guid.Empty; EventTime = DateTime.UtcNow }

    let inline simpleHandler f = 
        let withMetadata = f >> (fun x -> (x, emptyMetadata ()))
        Eventful.AggregateActionBuilder.simpleHandler systemConfiguration stateBuilder withMetadata
    
    let inline buildCmdHandler f =
        f
        |> simpleHandler
        |> buildCmd

    let inline fullHandler s f =
        let withMetadata a b c =
            f a b c
            |> Choice.map (fun evts ->
                evts 
                |> List.map (fun x -> (x, emptyMetadata ()))
                |> List.toSeq
            )
        Eventful.AggregateActionBuilder.fullHandler systemConfiguration s withMetadata
