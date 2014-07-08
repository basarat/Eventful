﻿namespace Eventful.Raven

open Raven.Json.Linq
open Raven.Abstractions.Data

type ProjectedDocument<'TDocument> = ('TDocument * Raven.Json.Linq.RavenJObject * Raven.Abstractions.Data.Etag)

type IDocumentFetcher =
    abstract member GetDocument<'TDocument> : string -> Async<ProjectedDocument<'TDocument> option> 
    abstract member GetDocuments : (string * System.Type) seq -> Async<(string * System.Type * Option<obj * RavenJObject * Etag>) seq>

type DocumentWriteRequest = {
    DocumentKey : string
    Document : Lazy<Raven.Json.Linq.RavenJObject>
    Metadata : Lazy<Raven.Json.Linq.RavenJObject>
    Etag : Raven.Abstractions.Data.Etag
}