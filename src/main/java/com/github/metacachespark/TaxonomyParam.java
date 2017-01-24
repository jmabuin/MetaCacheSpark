package com.github.metacachespark;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by chema on 1/13/17.
 */
public class TaxonomyParam {

	private String path;
	private String nodesFile;
	private String namesFile;
	private String mergeFile;
	private ArrayList<String> mappingPreFiles;
	private ArrayList<String> mappingPostFiles;

	public TaxonomyParam(String path, String nodesFile, String namesFile, String mergeFile, ArrayList<String> mappingPreFiles, ArrayList<String> mappingPostFiles) {
		this.path = path;
		this.nodesFile = nodesFile;
		this.namesFile = namesFile;
		this.mergeFile = mergeFile;
		this.mappingPreFiles = mappingPreFiles;
		this.mappingPostFiles = mappingPostFiles;
	}

	public TaxonomyParam( String inputPath, String taxpostmap) {

		this.path = inputPath;

		if(!this.path.isEmpty() && this.path.toCharArray()[this.path.length()] != '/') {
			this.path += "/";
		}

		this.nodesFile = this.path + "nodes.dmp";
		this.namesFile = this.path + "names.dmp";
		this.mergeFile = this.path + "merged.dmp";

		this.mappingPreFiles.add("assembly_summary.txt");

		//manually added accession to taxon map file names
		String postm = taxpostmap;

		if(!postm.isEmpty()) {
			this.mappingPostFiles.add(postm);
		}

		//default NCBI accession to taxon map file names
		this.mappingPostFiles.add(this.path + "nucl_gb.accession2taxid");
		this.mappingPostFiles.add(this.path + "nucl_wgs.accession2taxid");
		this.mappingPostFiles.add(this.path + "nucl_est.accession2taxid");
		this.mappingPostFiles.add(this.path + "nucl_gss.accession2taxid");

		//find additional maps by file extension ".accession2taxid"
		for(String f : FilesysUtility.files_in_directory(this.path, 0)) {
			if(f.contains(".accession2taxid")) {
				this.mappingPostFiles.add(f);
			}
		}

	}

	public TaxonomyParam() {

	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getNodesFile() {
		return nodesFile;
	}

	public void setNodesFile(String nodesFile) {
		this.nodesFile = nodesFile;
	}

	public String getNamesFile() {
		return namesFile;
	}

	public void setNamesFile(String namesFile) {
		this.namesFile = namesFile;
	}

	public String getMergeFile() {
		return mergeFile;
	}

	public void setMergeFile(String mergeFile) {
		this.mergeFile = mergeFile;
	}

	public ArrayList<String> getMappingPreFiles() {
		return mappingPreFiles;
	}

	public void setMappingPreFiles(ArrayList<String> mappingPreFiles) {
		this.mappingPreFiles = mappingPreFiles;
	}

	public ArrayList<String> getMappingPostFiles() {
		return mappingPostFiles;
	}

	public void setMappingPostFiles(ArrayList<String> mappingPostFiles) {
		this.mappingPostFiles = mappingPostFiles;
	}
}
