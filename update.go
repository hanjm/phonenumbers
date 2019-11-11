package phonenumbers

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"bytes"

	"github.com/golang/protobuf/proto"
)

// InitAutoUpdateDaemon will start a daemon and interval update all metadata and then take effect atomic
func InitAutoUpdateDaemon(interval time.Duration, fileCacheDir string) {
	const defaultInterval = time.Hour * 24 // 每天更新
	if interval <= 0 {
		interval = defaultInterval
	}
	const defaultCacheDir = "/tmp/phonenumbersCacheDir"
	if fileCacheDir == "" {
		fileCacheDir = defaultCacheDir
	}
	err := os.MkdirAll(fileCacheDir, 0755)
	if err != nil && !os.IsExist(err) {
		log.Printf("[E]failed to fMkdirAll, err:%s, dir:%s", err, fileCacheDir)
	}
	err = initFromCache(fileCacheDir)
	if err != nil {
		log.Printf("[E]failed to initFromcache, err:%s, try force update phonenumbers metadata...", err)
		err := update(fileCacheDir)
		if err != nil {
			log.Printf("[E]failed to force update, err:%s", err)
		} else {
			log.Printf("[I]force update phonenumbers metadata, version:%s", getVersion())
		}
	} else {
		log.Printf("[I]initFromcache, version:%s", getVersion())
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				err := update(fileCacheDir)
				if err != nil {
					log.Printf("[E]failed to update, err:%s", err)
				} else {
					log.Printf("[I]update phonenumbers metadata, version:%s", getVersion())
				}
			}
		}
	}()
}

var (
	_version atomic.Value
)

func getVersion() string {
	if _version.Load() == nil {
		_version.Store("2019-11-12T00:00:00Z")
	}
	return _version.Load().(string)
}

type metadataRaw struct {
	MetadataData     string            `json:"metadata_data"`
	RegionMapData    string            `json:"region_map_data"`
	TimezoneMapData  string            `json:"timezone_map_data"`
	CarrierMapData   map[string]string `json:"carrier_map_data"`
	GeocodingMapData map[string]string `json:"geocoding_map_data"`
	Version          string            `json:"version"`
}

func atomicReplaceeMetadataVar(m *metadataRaw) {
	// atomic store
	_metadataData.Store(m.MetadataData)
	_regionMapData.Store(m.RegionMapData)
	_timezoneMapData.Store(m.TimezoneMapData)
	_carrierMapData.Store(m.CarrierMapData)
	_geocodingMapData.Store(m.GeocodingMapData)
	_version.Store(m.Version)
}

const (
	metadataCacheFilename = "phonenumbers_metadataCache.cache"
)

func readFromFileCache(m *metadataRaw, fileCacheDir string) error {
	fullFilename := filepath.Join(fileCacheDir, metadataCacheFilename)
	data, err := ioutil.ReadFile(fullFilename)
	if err != nil {
		err = fmt.Errorf("failed to readFile, err:%s, fullname:%s", err, fullFilename)
		return err
	}
	err = json.Unmarshal(data, m)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal, err:%s, data:%s", err, data)
		return err
	}
	return nil
}

func storeFileCache(m *metadataRaw, fileCacheDir string) error {
	fullFilename := filepath.Join(fileCacheDir, metadataCacheFilename)
	data, err := json.Marshal(m)
	if err != nil {
		err = fmt.Errorf("failed to marshal, err:%s, m:%s", err, m)
		return err
	}
	err = ioutil.WriteFile(fullFilename, data, 0666)
	if err != nil {
		err = fmt.Errorf("failed to writeFile, err:%s, fullname:%s", err, fullFilename)
		return err
	}
	return nil
}

func initFromCache(fileCacheDir string) error {
	m := new(metadataRaw)
	err := readFromFileCache(m, fileCacheDir)
	if err != nil {
		return fmt.Errorf("readFromFileCache:%s", err)
	}
	atomicReplaceeMetadataVar(m)
	if err := loadDataAndAtomicReplaceVar(); err != nil {
		return fmt.Errorf("loadDataAndAtomicReplaceVar:%s", err)
	}
	return nil
}

func update(fileCacheDir string) error {
	m, err := fetchLatestMetadata(fileCacheDir)
	if err != nil {
		return fmt.Errorf("update metadata:%s", err)
	}
	err = storeFileCache(m, fileCacheDir)
	if err != nil {
		return fmt.Errorf("storeFileCache:%s", err)
	}
	atomicReplaceeMetadataVar(m)
	if err := loadDataAndAtomicReplaceVar(); err != nil {
		return fmt.Errorf("load metadata:%s", err)
	}
	return nil
}

func fetchLatestMetadata(fileCacheDir string) (m *metadataRaw, err error) {
	m = new(metadataRaw)
	metadata, metadataData, err := buildMetadata()
	if err != nil {
		return m, err
	}
	regionMapData, err := buildRegions(metadata)
	if err != nil {
		return m, err
	}
	timezoneMapData, err := buildTimezones()
	if err != nil {
		return m, err
	}
	carrierMapData, err := buildPrefixData(&carrier, fileCacheDir)
	if err != nil {
		return m, err
	}
	geocodingMapData, err := buildPrefixData(&geocoding, fileCacheDir)
	if err != nil {
		return m, err
	}
	m.MetadataData = metadataData
	m.RegionMapData = regionMapData
	m.TimezoneMapData = timezoneMapData
	m.CarrierMapData = carrierMapData
	m.GeocodingMapData = geocodingMapData
	m.Version = time.Now().Format(time.RFC3339)
	return m, nil
}

type prefixBuild struct {
	url     string
	dir     string
	srcPath string
	varName string
}

const (
	metadataURL = "https://raw.githubusercontent.com/googlei18n/libphonenumber/master/resources/PhoneNumberMetadata.xml"
	tzURL       = "https://raw.githubusercontent.com/googlei18n/libphonenumber/master/resources/timezones/map_data.txt"
)

var carrier = prefixBuild{
	url: "https://github.com/googlei18n/libphonenumber/trunk/resources/carrier",
	dir: "carrier",
}

var geocoding = prefixBuild{
	url: "https://github.com/googlei18n/libphonenumber/trunk/resources/geocoding",
	dir: "geocoding",
}

func buildMetadata() (*PhoneMetadataCollection, string, error) {
	log.Println("[I]Fetching PhoneNumberMetadata.xml from Github")
	body, err := fetchURL(metadataURL)
	if err != nil {
		return nil, "", err
	}

	log.Println("[I]Building new metadata collection")
	collection, err := BuildPhoneMetadataCollection(body, false, false)
	if err != nil {
		err = fmt.Errorf("error converting XML: %s", err)
		return nil, "", err
	}

	// write it out as a protobuf
	data, err := proto.Marshal(collection)
	if err != nil {
		err = fmt.Errorf("rrror marshalling metadata: %v", err)
	}
	return collection, gzipBytesAndBase64(data), nil
}

func fetchURL(url string) ([]byte, error) {
	resp, err := (&http.Client{
		Timeout: time.Minute,
	}).Get(url)
	if err != nil || resp.StatusCode != 200 {
		err = fmt.Errorf("error fetching URL '%s': %v", url, err)
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("error reading body: %s", err)
		return nil, err
	}
	return body, nil
}

func svnExport(dir string, url string) (err error) {
	_ = os.RemoveAll(dir)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	cmd := exec.Command(
		"/bin/bash",
		"-c",
		fmt.Sprintf("svn export %s --force", url),
	)
	cmd.Dir = dir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	_, err = ioutil.ReadAll(stderr)
	if err != nil {
		return err
	}
	outputBuf := bufio.NewReader(stdout)

	for {
		output, _, err := outputBuf.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		log.Println("[I]", string(output))
	}

	if err = cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func buildRegions(metadata *PhoneMetadataCollection) (string, error) {
	regionMap := BuildCountryCodeToRegionMap(metadata)
	result, err := intStringArrayMapToString(regionMap)
	if err != nil {
		return "", err
	}
	return result, nil
}

func buildTimezones() (string, error) {
	log.Println("[I]Building timezone map")
	body, err := fetchURL(tzURL)
	if err != nil {
		return "", err
	}

	// build our map of prefix to timezones
	prefixMap := make(map[int][]string)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}

		if strings.TrimSpace(line) == "" {
			continue
		}

		fields := strings.Split(line, "|")
		if len(fields) != 2 {
			err = fmt.Errorf("invalid format in timezone file: %s", line)
			return "", err
		}

		zones := strings.Split(fields[1], "&")
		if len(zones) < 1 {
			err = fmt.Errorf("invalid format in timezone file: %s", line)
			return "", err
		}

		// parse our prefix
		prefix, err := strconv.Atoi(fields[0])
		if err != nil {
			err = fmt.Errorf("invalid prefix in line: %s", line)
			return "", err
		}
		prefixMap[prefix] = zones
	}
	result, err := intStringArrayMapToString(prefixMap)
	if err != nil {
		return "", nil
	}
	return result, nil
}

func intStringArrayMapToString(prefixMap map[int][]string) (dst string, err error) {
	// build lists of our keys and values
	keys := make([]int, 0, len(prefixMap))
	values := make([]string, 0, 255)
	seenValues := make(map[string]bool, 255)

	for k, vs := range prefixMap {
		keys = append(keys, k)
		for _, v := range vs {
			_, seen := seenValues[v]
			if !seen {
				seenValues[v] = true
				values = append(values, v)
			}
		}
	}
	sort.Strings(values)
	sort.Ints(keys)

	internMap := make(map[string]int, len(values))
	for i, v := range values {
		internMap[v] = i
	}

	data := &bytes.Buffer{}

	// first write our values, as length of string and raw bytes
	joinedValues := strings.Join(values, "\n")
	if err := binary.Write(data, binary.LittleEndian, uint32(len(joinedValues))); err != nil {
		return "", err
	}
	if err := binary.Write(data, binary.LittleEndian, []byte(joinedValues)); err != nil {
		return "", err
	}

	// then the number of keys
	if err := binary.Write(data, binary.LittleEndian, uint32(len(keys))); err != nil {
		return "", err
	}

	// we write our key / value pairs as a varint of the difference of the previous prefix
	// and a uint16 of the value index
	last := 0
	intBuf := make([]byte, 6)
	for _, key := range keys {
		// first write our prefix
		diff := key - last
		l := binary.PutUvarint(intBuf, uint64(diff))
		if err := binary.Write(data, binary.LittleEndian, intBuf[:l]); err != nil {
			return "", err
		}

		// then our values
		values := prefixMap[key]

		// write our number of values
		if err := binary.Write(data, binary.LittleEndian, uint8(len(values))); err != nil {
			return "", err
		}

		// then each value as the interned index
		for _, v := range values {
			valueIntern := internMap[v]
			if err := binary.Write(data, binary.LittleEndian, uint16(valueIntern)); err != nil {
				return "", err
			}
		}

		last = key
	}
	return gzipBytesAndBase64(data.Bytes()), nil
}

func gzipBytesAndBase64(data []byte) string {
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	_, _ = w.Write(data)
	_ = w.Close()
	encoded := base64.StdEncoding.EncodeToString(compressed.Bytes())
	return encoded
}

func buildPrefixData(build *prefixBuild, fileCacheDir string) (map[string]string, error) {
	prefixDataMap := make(map[string]string)
	buildDir := filepath.Join(fileCacheDir, build.dir)
	log.Println("[I]Fetching " + build.url + " from Github to " + fileCacheDir)
	err := svnExport(buildDir, build.url)
	if err != nil {
		return nil, err
	}
	// get our top level language directories
	dirs, err := filepath.Glob(fmt.Sprintf("%s/*", buildDir))
	if err != nil {
		return nil, err
	}

	// for each directory
	languageMappings := make(map[string]map[int]string)
	for _, dir := range dirs {
		// only look at directories
		fi, _ := os.Stat(dir)
		if !fi.IsDir() {
			log.Printf("[I]Ignoring directory: %s\n", dir)
			continue
		}

		// get our language code
		parts := strings.Split(dir, "/")

		// build a map for that directory
		mappings, err := readMappingsForDir(dir)
		if err != nil {
			return nil, err
		}

		// save it for our language
		languageMappings[parts[1]] = mappings
	}

	for lang, mappings := range languageMappings {
		// iterate through our map, creating our full set of values and prefixes
		prefixes := make([]int, 0, len(mappings))
		seenValues := make(map[string]bool)
		values := make([]string, 0, 255)
		for prefix, value := range mappings {
			prefixes = append(prefixes, prefix)
			_, seen := seenValues[value]
			if !seen {
				values = append(values, value)
				seenValues[value] = true
			}
		}

		// make sure we won't overrun uint16s
		if len(values) > math.MaxUint16 {
			return nil, fmt.Errorf("too many values to represent in uint16")
		}

		// need sorted prefixes for our diff writing to work
		sort.Ints(prefixes)

		// sorted values compress better
		sort.Strings(values)

		// build our reverse mapping from value to offset
		internMappings := make(map[string]uint16)
		for i, value := range values {
			internMappings[value] = uint16(i)
		}

		// write our map
		data := &bytes.Buffer{}

		// first write our values, as length of string and raw bytes
		joinedValues := strings.Join(values, "\n")
		if err = binary.Write(data, binary.LittleEndian, uint32(len(joinedValues))); err != nil {
			return nil, err
		}
		if err = binary.Write(data, binary.LittleEndian, []byte(joinedValues)); err != nil {
			return nil, err
		}

		// then then number of prefix / value pairs
		if err = binary.Write(data, binary.LittleEndian, uint32(len(prefixes))); err != nil {
			return nil, err
		}

		// we write our prefix / value pairs as a varint of the difference of the previous prefix
		// and a uint16 of the value index
		last := 0
		intBuf := make([]byte, 6)
		for _, prefix := range prefixes {
			value := mappings[prefix]
			valueIntern := internMappings[value]
			diff := prefix - last
			l := binary.PutUvarint(intBuf, uint64(diff))
			if err = binary.Write(data, binary.LittleEndian, intBuf[:l]); err != nil {
				return nil, err
			}
			if err = binary.Write(data, binary.LittleEndian, uint16(valueIntern)); err != nil {
				return nil, err
			}

			last = prefix
		}

		var compressed bytes.Buffer
		w := gzip.NewWriter(&compressed)
		_, _ = w.Write(data.Bytes())
		_ = w.Close()
		c := base64.StdEncoding.EncodeToString(compressed.Bytes())
		prefixDataMap[lang] = c
	}

	return prefixDataMap, nil
}

func readMappingsForDir(dir string) (map[int]string, error) {
	log.Printf("[I]Building map for: %s\n", dir)
	mappings := make(map[int]string)

	files, err := filepath.Glob(dir + "/*.txt")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		body, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, err
		}
		items := strings.Split(file, "/")
		if len(items) != 3 {
			err = fmt.Errorf("file name %s not correct", file)
		}

		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Split(line, "|")
			if len(fields) != 2 {
				continue
			}
			prefix := fields[0]
			prefixInt, err := strconv.Atoi(prefix)
			if err != nil || prefixInt < 0 {
				err = fmt.Errorf("unable to parse line: %s", line)
				return nil, err
			}

			value := strings.TrimSpace(fields[1])
			if value == "" {
				log.Printf("[I]Ignoring empty value: %s", line)
			}

			_, repeat := mappings[prefixInt]
			if repeat {
				err = fmt.Errorf("repeated prefix for line: %s", line)
				return nil, err
			}
			mappings[prefixInt] = fields[1]
		}
	}

	log.Printf("[I]Read %d mappings in %s\n", len(mappings), dir)
	return mappings, nil
}
