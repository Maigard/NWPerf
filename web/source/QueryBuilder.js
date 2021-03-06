/*
 * Copyright 2013 Battelle Memorial Institute.
 * This software is licensed under the Battelle “BSD-style” open source license;
 * the full text of that license is available in the COPYING file in the root of the repository
 */
enyo.kind({
	name: "QueryBuilder",
	kind: "FittableRows",
	classes: "QueryBuilder",
	published: {
		allowUserSelect: false,
		userList: []
	},
	events: {
		onQueryChanged: ""
	},
	components: [
		{kind: "Scroller", fit: true, components: [
			{name: "queryList", onSetupItem: "newQueryRow", kind: "Repeater", components: [
				{kind: "FittableColumns", components: [
					{style: "padding-right: 10px;", components: [
						{kind: "onyx.Button", style: "min-width: 30px;", content: "+", ontap: "addQueryRow"},
						{kind: "onyx.Button", style: "min-width: 30px;", content: "-", name: "minusButton", ontap: "removeQueryRow"},
					]},
					{kind: "QueryItem", onQueryValueChanged: "updateQuery", onQueryServer: "queryServer"}
				]},
			]}
		]},
	],

	initialized: false,
	queryItems: [["Start Date"], ["End Date"]],

	create: function() {
		this.inherited(arguments);
		this.$.queryList.setCount(this.queryItems.length);
		this.initialized = true;
		this.queryServer();
	},

	newQueryRow: function(inSender, inEvent) {
		if(this.queryItems.length == 1 && inEvent.index == 0) {
			inEvent.item.$.minusButton.setDisabled(true);
		}
		inEvent.item.$.queryItem.setRowNumber(inEvent.index);
		inEvent.item.$.queryItem.setAllowUserSelect(this.allowUserSelect);
		if(inEvent.index >= this.queryItems.length) {
			this.queryItems[inEvent.item.$.queryItem.getRowNumber()] = inEvent.item.$.queryItem.getQueryValue();
		} else { 
			inEvent.item.$.queryItem.setQueryValue(this.queryItems[inEvent.index]);
		}
		return true;
	},

	addQueryRow: function(inSender, inEvent) {
		this.queryItems.splice(inEvent.index+1, 0, []);
		this.$.queryList.setCount(this.queryItems.length);
		this.queryServer();
	},

	removeQueryRow: function(inSender, inEvent) {
		this.queryItems.splice(inEvent.index, 1);
		this.$.queryList.setCount(this.$.queryList.count - 1);
		this.queryServer();
	},
	
	updateQuery: function(inSender, inEvent) {
		this.queryItems[inSender.getRowNumber()] = inSender.getQueryValue();
		/*
		if(this.initialized) {
			this.doQueryChanged(this.queryItems);
		}
		*/
	},
	allowUserSelectChanged: function(oldValue) {
		this.$.queryList.setCount(this.queryItems.length);
	},
	queryServer: function(inSender, inEvent) {
		this.doQueryChanged(this.queryItems);
	}
});

enyo.kind({
	name: "QueryItem",
	kind: "FittableColumns",
	published: {
		queryValue: [],
		rowNumber: -1,
		allowUserSelect: false
	},
	events: {
		onQueryValueChanged:"",
		onQueryServer:""
	},
	components: [
		{style: "min-width: 100px;", components: [
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", name: "queryType", onChange: "queryTypeSelected", maxHeight: "500", components: [
					{content: "Start Date"},
					{content: "End Date"},
					{content: "Submit Date"},
					{content: "Node Count"},
					{content: "Job Id"},
					{content: "Account"},
					{content: "Ran On Node"},
				]}
			]}
		]},
		{kind: "FittableColumns", name: "startDateItems", showing: false, components: [
			{style: "min-width: 75px;", components: [
				{kind: "onyx.PickerDecorator", components: [
					{},
					{kind: "onyx.Picker", name: "startDateBeforeAfter", onChange: "startDateChanged", components: [
						{content:"Before", name: "startDateBefore"},
						{content:"After", name: "startDateAfter"}
					]}
				]},
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "startDateChanged", name: "startMonth", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "startDateChanged", name: "startDay", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "startDateChanged", name: "startYear", maxHeight: "500"}
			]}
		]},
		{kind: "FittableColumns", name: "endDateItems", showing: false, components: [
			{style: "min-width: 75px;", components: [
				{kind: "onyx.PickerDecorator", components: [
					{},
					{kind: "onyx.Picker", name: "endDateBeforeAfter", onChange: "endDateChanged", components: [
						{content:"Before", name: "endDateBefore"},
						{content:"After", name: "endDateAfter"}
					]}
				]}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "endDateChanged", name: "endMonth", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "endDateChanged", name: "endDay", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "endDateChanged", name: "endYear", maxHeight: "500"}
			]},
		]},
		{kind: "FittableColumns", name: "submitDateItems", showing: false, components: [
			{style: "min-width: 75px;", components: [
				{kind: "onyx.PickerDecorator", components: [
					{},
					{kind: "onyx.Picker", name: "submitDateBeforeAfter", onChange: "submitDateChanged", components: [
						{content:"Before", name: "submitDateBefore"},
						{content:"After", name: "submitDateAfter"}
					]}
				]}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "submitDateChanged", name: "submitMonth", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "submitDateChanged", name: "submitDay", maxHeight: "500"}
			]},
			{kind: "onyx.PickerDecorator", components: [
				{},
				{kind: "onyx.Picker", onChange: "submitDateChanged", name: "submitYear", maxHeight: "500"}
			]}
		]},
		{kind: "FittableColumns", name: "nodeCountItems", showing: false, components: [
			{style: "min-width: 75px;", components: [
				{kind: "onyx.PickerDecorator", onChange: "nodeCountChanged", components: [
					{},
					{kind: "onyx.Picker", name: "nodeCountComparison", components: [
						{content: "<="},
						{content: ">="},
						{content: "=="},
						{content: "<"},
						{content: ">"}
					]}
				]}
			]},
			{kind: "onyx.InputDecorator", components: [
				{kind: "onyx.Input", name: "nodeCountNumber", oninput: "nodeCountChanged", placeholder: "Number of Nodes", type: "number"}
			]}
		]},
		{kind: "FittableColumns", name: "jobIdItems", showing: false, components: [
			{kind: "onyx.InputDecorator", components: [
				{kind: "onyx.Input", name: "jobId", oninput: "jobIdChanged", placeholder: "Job ID", type: "number"}
			]}
		]},
		{kind: "FittableColumns", showing: false, name: "accountItems", components: [
			{kind: "onyx.InputDecorator", components: [
				{kind: "onyx.Input", name: "account", oninput: "accountChanged"}
			]}
		]},
		{kind: "FittableColumns", showing: false, name: "onNodeItems", components: [
			{kind: "onyx.InputDecorator", components: [
				{kind: "onyx.Input", name: "onNode", oninput: "onNodeChanged"}
			]}
		]},
		{kind: "FittableColumns", showing: false, name: "userItems", components: [
			{kind: "onyx.InputDecorator", components: [
				{kind: "onyx.Input", name: "user", oninput: "userChanged"}
			]}
		]}
		
	],
	updatingQueryValue: false,
	activeControl: false,
	months: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
	create: function() {
		this.inherited(arguments);
		for(var i = 0; i < this.months.length; i++) {
			this.$.startMonth.createComponent({content: this.months[i]});
			this.$.endMonth.createComponent({content: this.months[i]});
			this.$.submitMonth.createComponent({content: this.months[i]});
		}
		for(i = 1; i <= 31; i++) {
			this.$.startDay.createComponent({content: i});
			this.$.endDay.createComponent({content: i});
			this.$.submitDay.createComponent({content: i});
		}
		for(i = 2005; i <= 2030; i++) {
			this.$.startYear.createComponent({content: i});
			this.$.endYear.createComponent({content: i});
			this.$.submitYear.createComponent({content: i});
		}
		this.render();
		this.setQueryValue([]);
	},
	queryTypeSelected: function(inSender, inEvent) {
		if (this.$.queryType.selected.content != this.queryValue[0]) {
			this.setQueryValue([this.$.queryType.selected.content]);
			this.doQueryValueChanged();
		}
	},
	startDateChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			var beforeAfter = {"After": ">", "Before": "<"}[this.$.startDateBeforeAfter.selected.content]
			this.queryValue = [	this.$.queryType.selected.content,
						beforeAfter,
						this.$.startMonth.selected.content,
						this.$.startDay.selected.content,
						this.$.startYear.selected.content];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	endDateChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			var beforeAfter = {"After": ">", "Before": "<"}[this.$.endDateBeforeAfter.selected.content]
			this.queryValue = [	this.$.queryType.selected.content,
						beforeAfter,
						this.$.endMonth.selected.content,
						this.$.endDay.selected.content,
						this.$.endYear.selected.content];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	submitDateChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			var beforeAfter = {"After": ">", "Before": "<"}[this.$.submitDateBeforeAfter.selected.content]
			this.queryValue = [	this.$.queryType.selected.content,
						beforeAfter,
						this.$.submitMonth.selected.content,
						this.$.submitDay.selected.content,
						this.$.submitYear.selected.content];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	nodeCountChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue && this.$.nodeCountNumber.getValue().match(/^[0-9]+/)) {
			this.queryValue = [	this.$.queryType.selected.content,
						this.$.nodeCountComparison.selected.content,
						this.$.nodeCountNumber.getValue()];
			this.doQueryValueChanged();
			if(this.queryValue != "") {
				this.doQueryServer();
			}
		}
	},
	jobIdChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue && this.$.jobId.getValue().match(/^[0-9]+/)) {
			this.queryValue = [	this.$.queryType.selected.content,
						this.$.jobId.getValue()];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	accountChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			this.queryValue = [	this.$.queryType.selected.content,
						this.$.account.getValue()];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	onNodeChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			this.queryValue = [	this.$.queryType.selected.content,
						this.$.onNode.getValue()];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	userChanged: function(inSender, inEvent) {
		if(!this.disableQueryValue) {
			this.queryValue = [	this.$.queryType.selected.content,
						this.$.user.getValue()];
			this.doQueryValueChanged();
			this.doQueryServer();
		}
	},
	queryValueChanged: function(oldValue) {
		this.disableQueryValue = true
		if(this.queryValue.length == 0) {
			this.queryValue = ["Start Date" ];
		}
		var components = this.$.queryType.getControls();
		for(i=0;i<components.length;i++) {
			if(components[i].content == this.queryValue[0]) {
				this.$.queryType.setSelected(components[i]);
			}
		}
		if(this.activeControl != false) {
			this.activeControl.setShowing(false);
		}
		var now = new Date();
		var then = new Date(now.valueOf() - 604800000);
		switch(this.queryValue[0]) {
			case "Start Date":
				this.activeControl = this.$.startDateItems;
				if(this.queryValue.length < 5) {
					this.queryValue = [this.queryValue[0], ">", this.months[then.getMonth()], then.getDate(), then.getFullYear()];
					this.doQueryValueChanged();
				}
				this.setPickerWithText(this.$.startDateBeforeAfter, {"<": "Before", ">": "After"}[this.queryValue[1]]);
				this.setPickerWithText(this.$.startMonth, this.queryValue[2]);
				this.setPickerWithText(this.$.startDay, this.queryValue[3]);
				this.setPickerWithText(this.$.startYear, this.queryValue[4]);
				break;
			case "End Date":
				this.activeControl = this.$.endDateItems;
				if(this.queryValue.length < 5) {
					this.queryValue = [this.queryValue[0], "<", this.months[now.getMonth()], now.getDate(), now.getFullYear()];
					this.doQueryValueChanged();
				}
				this.setPickerWithText(this.$.endDateBeforeAfter, {"<": "Before", ">": "After"}[this.queryValue[1]]);
				this.setPickerWithText(this.$.endMonth, this.queryValue[2]);
				this.setPickerWithText(this.$.endDay, this.queryValue[3]);
				this.setPickerWithText(this.$.endYear, this.queryValue[4]);
				break;
			case "Submit Date":
				this.activeControl = this.$.submitDateItems;
				if(this.queryValue.length < 5) {
					this.queryValue = [this.queryValue[0], ">", this.months[then.getMonth()], then.getDate(), then.getFullYear()];
					this.doQueryValueChanged();
				}
				this.setPickerWithText(this.$.submitDateBeforeAfter, {"<": "Before", ">": "After"}[this.queryValue[1]]);
				this.setPickerWithText(this.$.submitMonth, this.queryValue[2]);
				this.setPickerWithText(this.$.submitDay, this.queryValue[3]);
				this.setPickerWithText(this.$.submitYear, this.queryValue[4]);
				break;
			case "Node Count":
				this.activeControl = this.$.nodeCountItems;
				if(this.queryValue.length < 3) {
					this.queryValue = [this.queryValue[0], ">="];
					this.doQueryValueChanged();
				}
				this.setPickerWithText(this.$.nodeCountComparison, this.queryValue[1]);
				this.$.nodeCountNumber.setValue(this.queryValue[2]);
				break;
			case "Job Id":
				this.activeControl = this.$.jobIdItems;
				if(this.queryValue.length > 1) {
					this.$.jobId.setValue(this.queryValue[1]);
				}
				break;
			case "Account":
				this.activeControl = this.$.accountItems;
				if(this.queryValue.length > 1) {
					this.$.account.setValue(this.queryValue[1]);
				}
				break;
			case "Ran On Node":
				this.activeControl = this.$.onNodeItems;
				if(this.queryValue.length > 1) {
					this.$.onNode.setValue(this.queryValue[1]);
				}
				break;
			case "User":
				this.activeControl = this.$.userItems;
				if(this.queryValue.length > 1) {
					this.$.user.setValue(this.queryValue[1]);
				}
				break;
		}
		this.activeControl.setShowing(true);
		this.disableQueryValue = false;
	},
	setPickerWithText: function(component, selectText) {
		var components = component.getControls();
		for(var i = 0; i < components.length; i++) {
			if(components[i].content == selectText) {
				component.setSelected(components[i]);
			}
		}
	},
	allowUserSelectChanged: function(oldvalue) {
		if(this.allowUserSelect) {
			this.$.queryType.createComponent({content: "User"});
		}
	}
});
