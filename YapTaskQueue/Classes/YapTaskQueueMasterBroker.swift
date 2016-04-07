//
//  YapTaskQueueMasterBroker.swift
//  YapTaskQueue
//
//  Created by David Chiles on 4/5/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation

import YapDatabase.YapDatabaseView


///Only create one of these per database. This is required to filter out all the available actoins. The queue is managed in YapTaskQueueBroker
public class YapTaskQueueMasterBroker:YapDatabaseView {
    
    public convenience init(options:YapDatabaseViewOptions?) {
        
        let grouping = YapDatabaseViewGrouping.withObjectBlock { (transaction, collection, key, object) -> String? in
            guard let actionObject = object as? YapTaskQueueAction else {
                return nil
            }
            return actionObject.queueName()
        }
        
        let sorting = YapDatabaseViewSorting.withObjectBlock { (transaction, group, collection1, key1, object1, collection2, key2, object2) -> NSComparisonResult in
            guard let actionObject1 = object1 as? YapTaskQueueAction else {
                return .OrderedSame
            }
            guard let actionObject2 = object2 as? YapTaskQueueAction else {
                return .OrderedSame
            }
            
            return actionObject1.sort(actionObject2)
        }
        
        self.init(grouping: grouping, sorting: sorting, versionTag: nil, options: options)
    }
    
}