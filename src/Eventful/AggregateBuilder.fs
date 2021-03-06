﻿namespace Eventful

open System
open FSharpx.Choice
open FSharpx.Collections

open Eventful.EventStream

type CommandResult<'TMetadata> = Choice<list<string * obj * 'TMetadata>,NonEmptyList<CommandFailure>> 
type StreamNameBuilder<'TId> = ('TId -> string)

type IRegistrationVisitor<'T,'U> =
    abstract member Visit<'TCmd> : 'T -> 'U

type IRegistrationVisitable =
    abstract member Receive<'T,'U> : 'T -> IRegistrationVisitor<'T,'U> -> 'U

type EventResult = unit

type AggregateConfiguration<'TCommandContext, 'TEventContext, 'TAggregateId, 'TMetadata> = {
    // the combination of all the named state builders
    // for commands and events
    StateBuilder : CombinedStateBuilder<'TMetadata>

    GetCommandStreamName : 'TCommandContext -> 'TAggregateId -> string
    GetEventStreamName : 'TEventContext -> 'TAggregateId -> string
}

type ICommandHandler<'TEvent,'TId,'TCommandContext, 'TMetadata> =
    abstract member CmdType : Type
    abstract member AddStateBuilder : CombinedStateBuilder<'TMetadata> -> CombinedStateBuilder<'TMetadata>
    abstract member GetId : 'TCommandContext-> obj -> 'TId
                    // AggregateType -> Cmd -> Source Stream -> EventNumber -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TCommandContext -> obj -> EventStreamProgram<CommandResult<'TMetadata>,'TMetadata>
    abstract member Visitable : IRegistrationVisitable

type IEventHandler<'TEvent,'TId,'TMetadata> =
    abstract member AddStateBuilder : CombinedStateBuilder<'TMetadata> -> CombinedStateBuilder<'TMetadata>
    abstract member EventType : Type
                    // AggregateType -> Source Stream -> Source EventNumber -> Event -> -> Program
    abstract member Handler : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata> -> 'TEventContext -> string -> int -> EventStreamEventData<'TMetadata> -> EventStreamProgram<EventResult,'TMetadata>

type AggregateCommandHandlers<'TEvents,'TId,'TCommandContext,'TMetadata> = seq<ICommandHandler<'TEvents,'TId,'TCommandContext,'TMetadata>>
type AggregateEventHandlers<'TEvents,'TId,'TMetadata> = seq<IEventHandler<'TEvents,'TId,'TMetadata>>

type AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> private 
    (
        commandHandlers : list<ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata>>, 
        eventHandlers : list<IEventHandler<'TEvent,'TId,'TMetadata>>
    ) =
    member x.CommandHandlers = commandHandlers
    member x.EventHandlers = eventHandlers
    member x.AddCommandHandler handler = 
        new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(handler::commandHandlers, eventHandlers)
    member x.AddEventHandler handler = 
        new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(commandHandlers, handler::eventHandlers)
    member x.Combine (y:AggregateHandlers<_,_,_,_,_>) =
        new AggregateHandlers<_,_,_,_,_>(
            List.append commandHandlers y.CommandHandlers, 
            List.append eventHandlers y.EventHandlers)

    static member Empty = new AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>(List.empty, List.empty)
    
type IHandler<'TEvent,'TId,'TCommandContext,'TEventContext> = 
    abstract member add : AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> -> AggregateHandlers<'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata>

open FSharpx
open Eventful.Validation

type Validator<'TCmd,'TState> = 
| CommandValidator of ('TCmd -> seq<ValidationFailure>)
| StateValidator of ('TState option -> seq<ValidationFailure>)
| CombinedValidator of ('TCmd -> 'TState option -> seq<ValidationFailure>)

type SystemConfiguration<'TMetadata> = {
    SetSourceMessageId : string -> 'TMetadata -> 'TMetadata
    SetMessageId : Guid -> 'TMetadata -> 'TMetadata
}

type CommandHandler<'TCmd, 'TCommandContext, 'TCommandState, 'TId, 'TEvent,'TMetadata, 'TValidatedState> = {
    GetId : 'TCommandContext -> 'TCmd -> 'TId
    StateBuilder : NamedStateBuilder<'TCommandState, 'TMetadata>
    StateValidation : 'TCommandState option -> Choice<'TValidatedState, NonEmptyList<ValidationFailure>> 
    Validators : Validator<'TCmd,'TCommandState> list
    Handler : 'TValidatedState -> 'TCommandContext -> 'TCmd -> Choice<seq<'TEvent * 'TMetadata>, NonEmptyList<ValidationFailure>>
    SystemConfiguration : SystemConfiguration<'TMetadata>
}

open Eventful.EventStream
open Eventful.Validation

module AggregateActionBuilder =

    let log = createLogger "Eventful.AggregateActionBuilder"

    let fullHandler<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> systemConfiguration stateBuilder f =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = f
            SystemConfiguration = systemConfiguration
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata,'TState option> 

    let simpleHandler<'TId, 'TState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> systemConfiguration stateBuilder (f : 'TCmd -> ('TEvent * 'TMetadata)) =
        {
            GetId = (fun _ -> MagicMapper.magicId<'TId>)
            StateBuilder = stateBuilder
            Validators = List.empty
            StateValidation = Success
            Handler = (fun _ _ -> f >> Seq.singleton >> Success)
            SystemConfiguration = systemConfiguration
        } : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata,'TState option> 

    let toChoiceValidator cmd r =
        if r |> Seq.isEmpty then
            Success cmd
        else
            NonEmptyList.create (r |> Seq.head) (r |> Seq.tail |> List.ofSeq) |> Failure

    let runValidation validators cmd state =
        let v = new FSharpx.Validation.NonEmptyListValidation<ValidationFailure>()
        validators
        |> List.map (function
                        | CommandValidator validator -> validator cmd |> (toChoiceValidator cmd)
                        | StateValidator validator -> validator state |> (toChoiceValidator cmd)
                        | CombinedValidator validator -> validator cmd state |> (toChoiceValidator cmd))
         |> List.map (fun x -> x)
         |> List.fold (fun s validator -> v.apl validator s) (Choice.returnM cmd) 

    let untypedGetId<'TId,'TCmd,'TEvent,'TState, 'TCommandContext, 'TMetadata, 'TValidatedState> (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata, 'TValidatedState>) (context : 'TCommandContext) (cmd:obj) =
        match cmd with
        | :? 'TCmd as cmd ->
            sb.GetId context cmd
        | _ -> failwith <| sprintf "Invalid command %A" (cmd.GetType())

    let getUntypedChildState (combinedState : Map<string,obj> option) (childStateBuilder : INamedStateBuilder<'TMetadata>) = eventStream {
            let childState = 
                Option.maybe {
                    let! stateValue = combinedState
                    match stateValue |> Map.tryFind childStateBuilder.Name with
                    | Some hasValue ->
                        return hasValue
                    | None ->
                        return childStateBuilder.InitialState
                }

            return childState
        }

    let getChildState (combinedState : Map<string,obj> option) (childStateBuilder : NamedStateBuilder<'TChildState, 'TMetadata>) = eventStream {
        let! childState = getUntypedChildState combinedState childStateBuilder
        return childState |> Option.map (fun x -> x :?> 'TChildState)
    }

    let inline runCommand stream (eventsConsumed, combinedState) commandStateBuilder systemConfiguration f = 
        let unwrapper = MagicMapper.getUnwrapper<'TEvent>()

        eventStream {
            let! commandState = getChildState combinedState commandStateBuilder

            let result = 
                f commandState
                |> Choice.map (fun r ->
                    r
                    |> Seq.map (fun (e, m) -> (unwrapper e, m))
                    |> Seq.map (fun (evt, metadata) -> 
                                    let metadata = 
                                        metadata
                                        |> systemConfiguration.SetMessageId (Guid.NewGuid())
                                        |> systemConfiguration.SetSourceMessageId (Guid.NewGuid().ToString())
                                    (stream, evt, metadata))
                    |> List.ofSeq)

            return! 
                match result with
                | Choice1Of2 events -> 
                    eventStream {
                        for (stream, event, metadata) in events do
                            let! eventData = getEventStreamEvent event metadata
                            let expectedVersion = 
                                match eventsConsumed with
                                | 0 -> NewStream
                                | x -> AggregateVersion (x - 1)

                            let! writeResult = writeToStream stream expectedVersion (Seq.singleton eventData)

                            log.Debug <| lazy (sprintf "WriteResult: %A" writeResult)
                            
                            ()

                        let lastEvent  = (stream, eventsConsumed + (List.length events))
                        let lastEventNumber = (eventsConsumed + (List.length events) - 1)

                        return Choice1Of2 events
                    }
                | Choice2Of2 x ->
                    eventStream { return Choice2Of2 x }
        }

    let handleCommand 
        (commandHandler:CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata, 'TValidatedState>) 
        (aggregateConfiguration : AggregateConfiguration<_,_,_,_>) 
        (commandContext : 'TCommandContext) 
        (cmd : obj) =
        let processCommand cmd commandState =
            choose {
                let! validated = runValidation commandHandler.Validators cmd commandState
                let! validatedState = commandHandler.StateValidation commandState
                return! commandHandler.Handler validatedState commandContext validated
            }

        match cmd with
        | :? 'TCmd as cmd -> 
            let getId = FSharpx.Choice.protect (commandHandler.GetId commandContext) cmd
            match getId with
            | Choice1Of2 id ->
                let id = commandHandler.GetId commandContext cmd
                let stream = aggregateConfiguration.GetCommandStreamName commandContext id
                eventStream {
                    let! (eventsConsumed, combinedState) = 
                        aggregateConfiguration.StateBuilder 
                        |> CombinedStateBuilder.toStreamProgram stream
                    let! result = 
                        runCommand stream (eventsConsumed, combinedState) commandHandler.StateBuilder commandHandler.SystemConfiguration (processCommand cmd)
                    return
                        result
                        |> Choice.mapSecond (NonEmptyList.map CommandFailure.ofValidationFailure)
                }
            | Choice2Of2 exn ->
                eventStream {
                    return 
                        (Some "Retrieving aggregate id from command",exn)
                        |> CommandException
                        |> NonEmptyList.singleton 
                        |> Choice2Of2
                }
        | _ -> 
            eventStream {
                return 
                    (sprintf "Invalid command type: %A expected %A" (cmd.GetType()) typeof<'TCmd>)
                    |> CommandError
                    |> NonEmptyList.singleton 
                    |> Choice2Of2
            }
        
    let ToInterface (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata,'TValidatedState>) = {
        new ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata> with 
             member this.GetId context cmd = untypedGetId sb context cmd
             member this.CmdType = typeof<'TCmd>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add sb.StateBuilder
             member this.Handler aggregateConfig commandContext cmd = handleCommand sb aggregateConfig commandContext cmd
             member this.Visitable = {
                new IRegistrationVisitable with
                    member x.Receive a r = r.Visit<'TCmd>(a)
             }
        }

    let buildCmd (sb : CommandHandler<'TCmd, 'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata, 'TValidatedState>) : ICommandHandler<'TEvent,'TId,'TCommandContext,'TMetadata> = 
        ToInterface sb

    let addValidator 
        (validator : Validator<'TCmd,'TState>) 
        (handler: CommandHandler<'TCmd,'TCommandContext, 'TState, 'TId, 'TEvent,'TMetadata, 'TValidatedState>) = 
        { handler with Validators = validator::handler.Validators }

    let ensureFirstCommand x = addValidator (StateValidator (isNone id (None, "Must be the first command"))) x

    let buildSimpleCmdHandler<'TId,'TCmd,'TCmdState,'TEvent, 'TCommandContext,'TMetadata> emptyMetadata stateBuilder = 
        (simpleHandler<'TId,'TCmdState,'TCmd,'TEvent, 'TCommandContext,'TMetadata> emptyMetadata stateBuilder) >> buildCmd
        
    let getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId,'TMetadata> systemConfiguration (fId : 'TLinkEvent -> 'TId) metadata = {
        new IEventHandler<'TEvent,'TId,'TMetadata> with 
             member this.EventType = typeof<'TLinkEvent>
             member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder
             member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber (evt : EventStreamEventData<'TMetadata>) = eventStream {
                let metadata =  
                    metadata
                    |> systemConfiguration.SetSourceMessageId (Guid.NewGuid().ToString())
                    |> systemConfiguration.SetMessageId (Guid.NewGuid())

                let resultingStream = aggregateConfig.GetEventStreamName eventContext (fId (evt.Body :?> 'TLinkEvent))

                // todo: should not be new stream
                let! _ = EventStream.writeLink resultingStream NewStream sourceStream sourceEventNumber metadata
                return ()
             }
        }

    let getEventInterfaceForOnEvent<'TOnEvent, 'TEvent, 'TId, 'TState, 'TMetadata> (fId : 'TOnEvent -> 'TId) systemConfiguration (stateBuilder : NamedStateBuilder<'TState, 'TMetadata>) (runEvent : 'TOnEvent -> seq<'TEvent * 'TMetadata>) = {
        new IEventHandler<'TEvent,'TId,'TMetadata> with 
            member this.EventType = typeof<'TOnEvent>
            member this.AddStateBuilder aggregateStateBuilder = aggregateStateBuilder |> CombinedStateBuilder.add stateBuilder
            member this.Handler aggregateConfig eventContext sourceStream sourceEventNumber evt = eventStream {
                let unwrapper = MagicMapper.getUnwrapper<'TEvent>()
                let typedEvent = evt.Body :?> 'TOnEvent
                let resultingStream = aggregateConfig.GetEventStreamName eventContext (fId typedEvent)

                let! (eventsConsumed, combinedState) = 
                    aggregateConfig.StateBuilder |> CombinedStateBuilder.toStreamProgram resultingStream
                let! state = getChildState combinedState stateBuilder

                let! eventTypeMap = getEventTypeMap()

                let resultingEvents = 
                    runEvent typedEvent
                    |> Seq.map (fun (x,metadata) -> 
                        let metadata =  
                            metadata
                            |> systemConfiguration.SetSourceMessageId (Guid.NewGuid().ToString())
                            |> systemConfiguration.SetMessageId (Guid.NewGuid())

                        let event = unwrapper x
                        let eventType = eventTypeMap.FindValue (new ComparableType(event.GetType()))
                        Event { Body = event; EventType = eventType; Metadata = metadata })

                let! _ = EventStream.writeToStream resultingStream NewStream resultingEvents
                return ()
            }
    }

    let linkEvent<'TLinkEvent,'TEvent,'TId,'TCommandContext,'TEventContext,'TMetadata> systemConfiguration fId (linkEvent : 'TLinkEvent -> 'TEvent) (metadata:'TMetadata) = 
        getEventInterfaceForLink<'TLinkEvent,'TEvent,'TId,'TMetadata> systemConfiguration fId metadata

    let onEvent<'TOnEvent,'TEvent,'TEventState,'TId, 'TMetadata> systemConfiguration fId (stateBuilder : NamedStateBuilder<'TEventState, 'TMetadata>) (runEvent : 'TOnEvent -> seq<'TEvent * 'TMetadata>) = 
        getEventInterfaceForOnEvent<'TOnEvent,'TEvent,'TId,'TEventState, 'TMetadata> fId systemConfiguration stateBuilder runEvent

type AggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata> = {
    Configuration : AggregateConfiguration<'TCommandContext, 'TEventContext, 'TId, 'TMetadata>
    Handlers : AggregateHandlers<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
}

module Aggregate = 
    let toAggregateDefinition<'TEvents, 'TId, 'TCommandContext, 'TEventContext, 'TMetadata>
        (getCommandStreamName : 'TCommandContext -> 'TId -> string)
        (getEventStreamName : 'TEventContext -> 'TId -> string) 
        (commandHandlers : AggregateCommandHandlers<'TEvents,'TId,'TCommandContext, 'TMetadata>)
        (eventHandlers : AggregateEventHandlers<'TEvents,'TId,'TMetadata>) = 

            let commandStateBuilders = 
                commandHandlers |> Seq.map (fun x -> x.AddStateBuilder)
            let eventStateBuilders =
                eventHandlers |> Seq.map (fun x -> x.AddStateBuilder)

            let combinedAggregateStateBuilder = 
                commandStateBuilders
                |> Seq.append eventStateBuilders
                |> Seq.fold (|>) CombinedStateBuilder.empty

            let config = {
                StateBuilder = combinedAggregateStateBuilder
                GetCommandStreamName = getCommandStreamName
                GetEventStreamName = getEventStreamName
            }

            let handlers =
                commandHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddCommandHandler h) AggregateHandlers.Empty

            let handlers =
                eventHandlers |> Seq.fold (fun (x:AggregateHandlers<_,_,_,_,_>) h -> x.AddEventHandler h) handlers

            {
                Configuration = config
                Handlers = handlers
            }