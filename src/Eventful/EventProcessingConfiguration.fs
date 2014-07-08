﻿namespace Eventful

open System

type IStateBuilder<'TState,'TItem> =
    inherit IComparable
    abstract member Fold : 'TState -> 'TItem -> 'TState
    abstract member Zero : 'TState
    abstract member Types : seq<Type>
    abstract member Name : string
    abstract member Version : string
    abstract member StateType : Type

type AggregateStateBuilder<'TState, 'TItem> = {
    zero : 'TState
    fold : 'TState -> 'TItem -> 'TState
    name : string
    version : string
    types : seq<Type>
} with  
    static member ToUntypedInterface<'TState,'TItem> (sb : AggregateStateBuilder<'TState,'TItem>) = {
            new IStateBuilder<obj,obj> with 
             member this.Fold (state : obj) (evt : obj) = 
                match state with
                | :? 'TState as s ->    
                    let result =  sb.fold s (evt :?> 'TItem)
                    result :> obj
                | _ -> state
             member this.Zero = sb.zero :> obj
             member this.Name = sb.name
             member this.Types = sb.types
             member this.Version = sb.version
             member this.StateType = typeof<'TState>
            interface IComparable with
                member this.CompareTo(obj) =
                    match obj with
                    | :? IStateBuilder<'TState,'TItem> as sc -> compare sb.name sc.Name
                    | _ -> -1
    }

type cmdHandler = obj -> (string * IStateBuilder<obj,obj> * (obj -> Choice<seq<obj>, seq<string>>))

type EventProcessingConfiguration = {
    CommandHandlers : Map<string, (Type * cmdHandler)>
    StateBuilders: Set<IStateBuilder<obj,obj>>
    EventHandlers : Map<string, (string *  seq<(obj -> seq<(string *  IStateBuilder<obj,obj> * (obj -> seq<obj>))>)>)>
    TypeToTypeName : Type -> string
    TypeNameToType : string -> Type
}
with 
    static member Empty = 
        { 
            CommandHandlers = Map.empty
            StateBuilders = Set.empty
            EventHandlers = Map.empty 
            TypeToTypeName = (fun t -> t.FullName)
            TypeNameToType = System.Type.GetType 
        } 

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EventProcessingConfiguration =
    let addCommand<'TCmd, 'TState> (toId : 'TCmd -> IIdentity) (stateBuilder : AggregateStateBuilder<'TState,obj>) (handler : 'TCmd -> 'TState -> Choice<seq<obj>, seq<string>>) (config : EventProcessingConfiguration) = 
        let cmdType = typeof<'TCmd>.FullName
        let outerHandler (cmdObj : obj) =
            let realHandler (cmd : 'TCmd) =
                let stream = (toId cmd).GetId
                let realRealHandler = 
                    let blah = handler cmd
                    fun (state : obj) ->
                        blah (state :?> 'TState)
                let untypedStateBuilder = AggregateStateBuilder<_,_>.ToUntypedInterface stateBuilder
                (stream, untypedStateBuilder, realRealHandler)
            match cmdObj with
            | :? 'TCmd as cmd -> realHandler cmd
            | _ -> failwith <| sprintf "Unexpected command type: %A" (cmdObj.GetType())
        config
        |> (fun config -> { config with CommandHandlers = config.CommandHandlers |> Map.add cmdType (typeof<'TCmd>, outerHandler) })
    let addEvent<'TEvt, 'TState> (toId: 'TEvt -> string seq) (stateBuilder : AggregateStateBuilder<'TState,obj>) (handler : 'TEvt -> 'TState -> seq<obj>) (config : EventProcessingConfiguration) =
        let evtType = config.TypeToTypeName typeof<'TEvt>
        let outerHandler (evtObj : obj) : seq<(string * IStateBuilder<obj,obj> * (obj -> seq<obj>))> =
            let realHandler (evt : 'TEvt) : seq<(string * IStateBuilder<obj,obj> * (obj -> seq<obj>))> =
                toId evt
                |> Seq.map (fun stream ->
                    let realRealHandler = 
                        let blah = handler evt
                        fun (state : obj) ->
                            blah (state :?> 'TState)
                    let untypedStateBuilder = AggregateStateBuilder<_,_>.ToUntypedInterface stateBuilder
                    (stream, untypedStateBuilder, realRealHandler))
            match evtObj with
            | :? 'TEvt as evt -> realHandler evt
            | _ -> failwith <| sprintf "Unexpected event type: %A" (evtObj.GetType())
        match config.EventHandlers |> Map.tryFind evtType with
        | Some (_, existing) -> { config with EventHandlers = config.EventHandlers |> Map.add evtType (config.TypeToTypeName typeof<'TEvt>, existing |> Seq.append (Seq.singleton outerHandler)) }
        | None ->  { config with EventHandlers = config.EventHandlers |> Map.add evtType (config.TypeToTypeName typeof<'TEvt>, Seq.singleton outerHandler) }