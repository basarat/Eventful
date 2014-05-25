﻿namespace Eventful

type LastCompleteItemMessage<'TItem> = 
|    Start of ('TItem * AsyncReplyChannel<unit>) 
|    Complete of 'TItem

type LastCompleteItemAgent<'TItem when 'TItem : equality> () = 
    let mutable lastComplete = None
    let agent = Agent.Start(fun agent ->

        let rec loop state = async {
            let! msg = agent.Receive()
            match msg with
            | Start (item, reply) ->
                reply.Reply()
                let state' = state |> LastCompleteItemTracker.start item
                return! loop state'
            | Complete item ->
                let state' = state |> LastCompleteItemTracker.complete item
                lastComplete <- state'.LastComplete
                return! loop state'
        }

        loop LastCompleteItemTracker<'TItem>.Empty
    )

    member x.LastComplete () : 'TItem option =
        lastComplete

    member x.Start(item, ?timeout) = 
      agent.PostAndAsyncReply((fun ch -> Start (item,ch)), ?timeout=timeout)

    member x.Complete(item) = 
      agent.Post(Complete item)