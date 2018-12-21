/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

package com.github.jmabuin.metacachespark.io;

import cz.adamh.utils.NativeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Serializable;

public class ExtractionFunctions implements Serializable {

    private static final Log LOG = LogFactory.getLog(ExtractionFunctions.class);

    public  ExtractionFunctions() {

        try {
            NativeUtils.loadLibraryFromJar("/libextraction.so");
            LOG.warn("Native library loaded");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!! : "+e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.warn("Error!! Could not load the native library!!! : "+e.getMessage());
        }


    }

    public String extract_sequence_id(String text) {
        String sequid = this.extract_ncbi_accession_version_number(text);
        if(!sequid.isEmpty()) {
            return sequid;
        }

        sequid = this.extract_genbank_identifier(text);
        if(!sequid.isEmpty()) {
            return sequid;
        }

        return this.extract_ncbi_accession_number(text);
    }

    //Native methods
    public native String extract_ncbi_accession_version_number(String text);
    public native String extract_genbank_identifier(String text);
    public native String extract_ncbi_accession_number(String text);

}
