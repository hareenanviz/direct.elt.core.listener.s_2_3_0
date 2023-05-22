package com.anvizent.elt.core.listener;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorBlacklisted;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerExecutorUnblacklisted;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerNodeBlacklisted;
import org.apache.spark.scheduler.SparkListenerNodeUnblacklisted;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;

import com.anvizent.elt.core.listener.common.ApplicationListenerUtil;
import com.anvizent.elt.core.listener.common.bean.ApplicationBean;
import com.anvizent.elt.core.listener.common.bean.Component;
import com.anvizent.elt.core.listener.common.exception.ListenerInstanceAlreadyExistsException;
import com.anvizent.rest.util.RestUtil;

/**
 * @author Hareen Bejjanki
 *
 */
public class ApplicationListener implements SparkListenerInterface {

	private static ApplicationListener applicationListenerInstance;

	private RestUtil restUtil;
	private final LinkedHashMap<String, Component> components = new LinkedHashMap<>();
	private final LinkedHashMap<String, ArrayList<String>> componentSourceOf = new LinkedHashMap<>();

	/**
	 * Only to be called by spark.
	 */
	public ApplicationListener(SparkConf sparkConf) {
		if (ApplicationBean.getInstance().getStatsSettingsStore() != null || ApplicationBean.getInstance().getJobSettingStore() != null) {
			restUtil = new RestUtil();
		}

		if (applicationListenerInstance == null) {
			applicationListenerInstance = this;
		}
	}

	public static ApplicationListener getInstance() throws ListenerInstanceAlreadyExistsException {
		if (applicationListenerInstance == null) {
			throw new ListenerInstanceAlreadyExistsException();
		}

		return applicationListenerInstance;
	}

	public LinkedHashMap<String, Component> getComponents() {
		return components;
	}

	public LinkedHashMap<String, ArrayList<String>> getComponentSourceOf() {
		return componentSourceOf;
	}

	public void addComponentSourceOf(String componentName, String sourceName) {
		if (!componentSourceOf.containsKey(componentName)) {
			componentSourceOf.put(componentName, new ArrayList<>());
		}

		componentSourceOf.get(componentName).add(sourceName);
	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
		ApplicationListenerUtil.onApplicationEnd(restUtil, components);
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart arg0) {
		ApplicationListenerUtil.onApplicationStart(arg0, restUtil);
	}

	@Override
	public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {
		ApplicationListenerUtil.onExecutorAdded(arg0, restUtil);
	}

	@Override
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onJobEnd(SparkListenerJobEnd arg0) {
		ApplicationListenerUtil.onJobEnd(components);
	}

	@Override
	public void onJobStart(SparkListenerJobStart arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onOtherEvent(SparkListenerEvent arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onStageCompleted(SparkListenerStageCompleted arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onStageSubmitted(SparkListenerStageSubmitted arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onTaskEnd(SparkListenerTaskEnd arg0) {
		ApplicationListenerUtil.onTaskEnd(restUtil);
	}

	@Override
	public void onTaskGettingResult(SparkListenerTaskGettingResult arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onTaskStart(SparkListenerTaskStart arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onNodeBlacklisted(SparkListenerNodeBlacklisted arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted arg0) {
		// TODO Auto-generated method stub

	}
}
