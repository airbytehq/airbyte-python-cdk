# AsyncHttpJobRepository sequence diagram

- Components marked as optional are not required and can be ignored.
- if `url_requester` is not provided, `urls_extractor` will get urls from the `polling_job_response`
- interpolation_context, e.g. `create_job_response` or `polling_job_response` can be obtained from stream_slice

```mermaid
---
title: AsyncHttpJobRepository Sequence Diagram
---
sequenceDiagram
    participant AsyncHttpJobRepository as AsyncOrchestrator
    participant CreationRequester as creation_requester
    participant PollingRequester as polling_requester
    participant UrlRequester as url_requester (Optional)
    participant DownloadRetriever as download_retriever
    participant AbortRequester as abort_requester (Optional)
    participant DeleteRequester as delete_requester (Optional)
    participant Reporting Server as Async Reporting Server

    AsyncHttpJobRepository ->> CreationRequester: Initiate job creation
    CreationRequester ->> Reporting Server: Create job request
    Reporting Server -->> CreationRequester: Job ID response
    CreationRequester -->> AsyncHttpJobRepository: Job ID

    loop Poll for job status
        AsyncHttpJobRepository ->> PollingRequester: Check job status
        PollingRequester ->> Reporting Server: Status request (interpolation_context: `create_job_response`)
        Reporting Server -->> PollingRequester: Status response
        PollingRequester -->> AsyncHttpJobRepository: Job status
    end

    alt Status: Ready
        AsyncHttpJobRepository ->> UrlRequester: Request download URLs (if applicable)
        UrlRequester ->> Reporting Server: URL request (interpolation_context: `polling_job_response`)
        Reporting Server -->> UrlRequester: Download URLs
        UrlRequester -->> AsyncHttpJobRepository: Download URLs

        AsyncHttpJobRepository ->> DownloadRetriever: Download reports
        DownloadRetriever ->> Reporting Server: Retrieve report data (interpolation_context: `url`)
        Reporting Server -->> DownloadRetriever: Report data
        DownloadRetriever -->> AsyncHttpJobRepository: Report data
    else Status: Failed
        AsyncHttpJobRepository ->> AbortRequester: Send abort request
        AbortRequester ->> Reporting Server: Abort job
        Reporting Server -->> AbortRequester: Abort confirmation
        AbortRequester -->> AsyncHttpJobRepository: Confirmation
    end

    AsyncHttpJobRepository ->> DeleteRequester: Send delete job request
    DeleteRequester ->> Reporting Server: Delete job
    Reporting Server -->> DeleteRequester: Deletion confirmation
    DeleteRequester -->> AsyncHttpJobRepository: Confirmation


```
