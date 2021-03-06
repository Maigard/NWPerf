/*
 * Copyright 2013 Battelle Memorial Institute.
 * This software is licensed under the Battelle “BSD-style” open source license;
 * the full text of that license is available in the COPYING file in the root of the repository
 */
enyo.kind({
        name: "JobManager",
	kind: "Component",
	events: {
		onNewJobList: "",
		onNewJob: ""
	},
	jobListTag: 0,
	jobListReq: false,
	getJobList: function(query) {
		this.jobListReq = new enyo.Ajax({url: "jobs/"})
		.response(this, function(inSender, inResponse) {
			if(parseInt(inResponse.tag) == this.jobListTag) {
				this.doNewJobList(inResponse.jobs);
			}
		})
		.go({tag: ++this.jobListTag, q: JSON.stringify(query)});
		return this.jobListReq;
	},
	jobTag: 0,
	jobReq: false,
	getJob: function(jobId) {
		this.jobReq = new enyo.Ajax({url: "jobs/"+jobId})
		.response(this, function(inSender, inResponse) {
			if(parseInt(inResponse.tag) == this.jobTag) {
				delete inResponse.tag
				this.doNewJob(inResponse);
			}
		})
		.go({tag: ++this.jobTag});
		return this.jobReq;
	}
});
