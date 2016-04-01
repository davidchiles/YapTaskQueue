//
//  YapTaskQueueExtension.swift
//  YapTaskQueue
//
//  Created by David Chiles on 3/31/16.
//  Copyright © 2016 David Chiles. All rights reserved.
//

import Foundation
import YapDatabase.YapDatabaseView

public class YapTaskQueueExtension: YapDatabaseView {
    
    private var databaseConnection:YapDatabaseConnection? = nil
    private let operationQueue = NSOperationQueue()
    
    public convenience init(options:YapDatabaseViewOptions) {
        
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
        self.addObserver(self, forKeyPath: "registeredDatabase", options: .New, context: nil)
    }
    
    deinit {
        self.removeObserver(self, forKeyPath: "registeredDatabase")
    }
    
    override public func observeValueForKeyPath(keyPath: String?, ofObject object: AnyObject?, change: [String : AnyObject]?, context: UnsafeMutablePointer<Void>) {
        self.didRegisterExtension()
    }
    
    func didRegisterExtension() {
        
        NSNotificationCenter.defaultCenter().addObserverForName(YapDatabaseModifiedNotification, object: self.registeredDatabase, queue: self.operationQueue) {[weak self] (notification) in
            if let strongSelf = self {
                strongSelf.processDatabaseNotification(notification)
            }
        }
        
        self.databaseConnection = self.registeredDatabase?.newConnection()
    }
    
    func processDatabaseNotification(notification:NSNotification) {
        
        guard let viewName = self.registeredName else {
            return
        }
        
        guard let connection = self.databaseConnection?.ext(viewName) as? YapDatabaseViewConnection else {
            return
        }
        
        if connection.hasChangesForNotifications([notification]) {
            self.checkForActions()
        }
    }
    
    func checkForActions() {
        self.databaseConnection?.asyncReadWriteWithBlock({ (transaction) in
            guard let name = self.registeredName else {
                return
            }
            
            let viewTransaction = transaction.ext(name) as? YapDatabaseViewTransaction
            guard let groups = viewTransaction?.allGroups() else {
                return
            }
            
            for groupName in groups {
                viewTransaction?.enumerateKeysAndObjectsInGroup(groupName, usingBlock: { (collection, key, object, row, stop) in
                    guard let action = (object as? YapTaskQueueAction)?.action() else {
                        return
                    }
                    do {
                        try action()
                    } catch {
                        
                    }
                    
                })
            }
        })
    }
}