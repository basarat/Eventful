﻿namespace Eventful

open System
open FSharpx
open FSharpx.Validation
open FSharpx.Collections

type ValidationFailure = string

module Validation = 
    let Success = Choice1Of2
    let Failure = Choice2Of2

    let notBlank (f:'A -> string) fieldName (x:'A) : seq<ValidationFailure> = 
        if f x |> String.IsNullOrWhiteSpace then
            sprintf "%s must not be blank" fieldName |> Seq.singleton
        else
            Seq.empty

    let isNone (f:'A -> 'B option) msg (x:'A) : seq<ValidationFailure> = 
        if f x |> Option.isNone then
            Seq.empty
        else
            msg |> Seq.singleton