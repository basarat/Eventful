﻿namespace Eventful.EventStore 

open Eventful
open Eventful.EventStream
open FSharpx.Collections
open FSharpx.Option
open EventStore.ClientAPI

module EventStreamInterpreter = 
    let interpret<'A,'TMetadata when 'TMetadata : equality> 
        (eventStore : Client) 
        (serializer : ISerializer) 
        (eventTypeMap : Bimap<string, ComparableType>) 
        (prog : FreeEventStream<obj,'A,'TMetadata>) : Async<'A> = 
        let rec loop prog (values : Map<EventToken,(byte[]*byte[])>) (writes : Vector<string * int * obj * 'TMetadata>) : Async<'A> =
            match prog with
            | FreeEventStream (GetEventTypeMap ((), f)) ->
                let next = f eventTypeMap
                loop next values writes
            | FreeEventStream (ReadFromStream (stream, eventNumber, f)) -> 
                async {
                    let! event = eventStore.readEvent stream eventNumber
                    let readEvent = 
                        if(event.Status = EventReadStatus.Success) then
                            let event = event.Event.Value.Event
                            let eventToken = {
                                Stream = stream
                                Number = eventNumber
                                EventType = event.EventType
                            }
                            Some (eventToken, (event.Data, event.Metadata))
                        else
                            None

                    match readEvent with
                    | Some (eventToken, evt) -> 
                        let next = f (Some eventToken)
                        let values' = values |> Map.add eventToken evt
                        return! loop next values' writes
                    | None ->
                        let next = f None
                        return! loop next values writes
                }
            | FreeEventStream (ReadValue (token, g)) ->
                let (data, metadata) = values.[token]
                let typeName = (eventTypeMap.Find token.EventType).RealType.FullName
                let dataObj = serializer.DeserializeObj(data) typeName
                let metadataObj = serializer.DeserializeObj(metadata) typeof<'TMetadata>.AssemblyQualifiedName :?> 'TMetatdata
                let next = g (dataObj,metadataObj)
                loop next  values writes
            | FreeEventStream (WriteToStream (streamId, eventNumber, events, next)) ->
                let toEventData = function
                    | Event { Body = dataObj; EventType = typeString; Metadata = metadata} -> 
                        let serializedData = serializer.Serialize(dataObj)
                        let serializedMetadata = serializer.Serialize(metadata)
                        new EventData(System.Guid.NewGuid(), typeString, true, serializedData, serializedMetadata) 
                    | EventLink (destinationStream, destinationEventNumber, metadata) ->
                        let bodyString = sprintf "%d@%s" destinationEventNumber destinationStream
                        let body = System.Text.Encoding.UTF8.GetBytes bodyString
                        let serializedMetadata = serializer.Serialize(metadata)
                        new EventData(System.Guid.NewGuid(), "$>", true, body, serializedMetadata) 

                let eventDataArray = 
                    events
                    |> Seq.map toEventData
                    |> Array.ofSeq

                async {
                    let esExpectedEvent = 
                        match eventNumber with
                        | Any -> -2
                        | NewStream -> -1
                        | AggregateVersion x -> x
                    let! writeResult = eventStore.append streamId esExpectedEvent eventDataArray
                    return! loop (next writeResult) values writes
                }
            | FreeEventStream (NotYetDone g) ->
                let next = g ()
                loop next values writes
            | Pure result ->
                async {
                    return result
                }
        loop prog Map.empty Vector.empty