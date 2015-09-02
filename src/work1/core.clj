(ns work1.core
  (:require [csv-map.core :refer [parse-csv write-csv]]
            [clojure.java.jdbc :as jdbc]
            [org.httpkit.server :as kit :refer [run-server]]
            [org.httpkit.client :as http]
            [clj-time.core :as time]
            [clj-time.format]
            [clojure.string :as s]
            [clojure.java.io :as io :refer [input-stream file]]
            [flake.core :as flake :refer [id]])
  (:import [java.security.MessageDigest]
           [java.math.BigInteger]))

(def temp-storage (atom {:colleges nil
                         :users nil}))

;; (swap! temp-storage assoc-in [:colleges 1] #{1 2 3})

(flake/init!)

(let [db-host "localhost"
      db-port 5432
      db-name "workdb"]
  (def db-spec {:subprotocol "postgresql"
                :subname (str "//"  db-host ":" db-port "/" db-name)
                :user "aaa"
                :password "123"}))

(defn md5 [s]
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        size (* 2 (.getDigestLength algorithm))
        raw (.digest algorithm (.getBytes s))
        sig (.toString (java.math.BigInteger. 1 raw) 16)
        padding (apply str (repeat (- size (count sig)) "0"))]
    (str padding sig)))

(defn sign [method uri date content-length password]
  (-> (s/join '& [method uri date content-length (md5 password)])
      md5))

(defn to-GMT []
  (clj-time.format/unparse (clj-time.format/formatter "EEE, dd MMM yyyy HH:mm:ss 'GMT'") (time/now)))


(defn insert-db [tablename arg-map]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/insert! conn tablename arg-map)))

(defn query-from-db [psql]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/query conn psql)))

(defn delete-from-db [tablename psql]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/delete! conn tablename psql)))

;; (defn string-to-int [map]
;;   (let [{:strs [virtual_college_id user_serial_id]} map]
;;     (conj map {"virtual_college_id" (Integer/parseInt virtual_colLege_id)
;;                "user_serial_id" (Integer/parseInt user_serial_id)})))

(defn s3 [s]
  (cond
    (= (count s) 1) (str "00" s)
    (= (count s) 2) (str "0" s)
    (= (count s) 3) (str s)))

(defn import-db [tablename file]
  (let [middle-file (read-string (slurp "last.txt"))]
    (let [result (->> (parse-csv (slurp file) :key :keyword)
                      (map #(assoc % :identity (id) :virtual_college_id (middle-file (:csv_college_id %)))))]
      (doseq [x result]
        (let [{:keys [csv_college_id csv_user_id]} x
              x (dissoc x :csv_college_id :csv_user_id)
              _ (prn x)
              new-row (first (insert-db tablename x))
              new-user-id (:id new-row)]
          (spit "data-rela.txt" (assoc (read-string (slurp "data-rela.txt")) [csv_college_id (s3 csv_user_id)] new-user-id))
          )))
    ))

(defn app [req]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body req})

;; (defn filename [local-path]
;;   (last (s/split local-path #"/")))

;; (defn get-path [file] (.getPath file))
;; (defn isfile? [file] (not (.isDirectory file)))

(defn DS_Store? [file]
  (let [extension (last (s/split file #"\."))]
    (if (= extension "DS_Store")
      true
      false)))

(defn all-file-path
  "得到一个文件夹下所有文件的绝对路径（包括子文件夹包含的文件）"
  [absolute-path]
  (let [middle-file (read-string (slurp "last.txt"))]
    (->> (io/file absolute-path)
         file-seq
         (filter #(not (.isDirectory %)))
         (map #(.getPath %))
         sort
         (filter #(not (DS_Store? %)))
         (filter #(> (compare % (:last-uploadfile middle-file)) 0))
         (filter #(= (count (s/split % #"/")) 8)))
    ))

(defn getFile [operator_id password path]
  (let [path-vec (s/split path #"/")
        uri (str "/" (path-vec 3) "/" (path-vec 4))
        date (to-GMT)
        result @(http/get path
                          {:headers {"Authorization" (str "UpYun " operator_id ":" (sign "GET" uri date 0 password))
                                     "Date" date}})
        {status :status body :body} result]
    (if (= status 200)
      {:ok body}
      {:error body})))

(defn deleteFile [operator_id password path]
  (let [path-vec (s/split path #"/")
        uri (str "/" (path-vec 3) "/" (path-vec 4))
        date (to-GMT)
        result @(http/delete path
                             {:headers {"Authorization" (str "UpYun " operator_id ":" (sign "DELETE" uri date 0 password))
                                        "Date" date}})
        _ (prn result)
        {status :status body :body} result]
    (if (= status 200)
      (do
        (delete-from-db :testpictures ["link = ?" path])
        {:ok nil})
      {:error body})))

(defn uploadFile [operator_id password path]
  (let [_ (prn path)
        date (to-GMT)
        file (file path)
        filelength (.length file)
        [_ _ _ _ _ csv-college-id csv-user-id filename] (s/split path #"/")
        extension (str "."(last (s/split filename #"\.")))
        link (str "/" (id) extension)
        uri (str "/test-hoolay" link)
        url (str "http://v0.api.upyun.com" uri)
        ;;_ (prn "Debug: " {:filename path})
        csv-art-id (first (take-last 2 (s/split filename #"[\－\-\_]+")))
        middle-file (read-string (slurp "data-rela.txt"))
        virtual-user-id (middle-file [csv-college-id (s3 csv-user-id)])]
    (when (= csv-art-id "") (prn path))
    (if (nil? virtual-user-id)
      (prn "users not exist: " csv-college-id "/" csv-user-id)
      (let [result @(http/put url
                              {:headers {"Authorization" (str "UpYun " operator_id ":" (sign "PUT" uri date filelength password))
                                         "Date" date
                                         "Content-Length" (str filelength)}
                               :body (input-stream file)})
            _ (prn result)]
        (if (= 1 1);;(= (:status result) 200)
          (let [{:keys [x-upyun-width x-upyun-height x-upyun-frames x-upyun-file-type]} (:headers result)
                csv-picture-id (first (s/split (last (s/split filename #"[-_]+")) #"\."))
                middle-file1 (read-string (slurp "last.txt"))]
            (when (not (and (= csv-college-id (middle-file1 :last-csv-college-id))
                            (= csv-user-id (middle-file1 :last-csv-user-id))
                            (= csv-art-id (middle-file1 :last-csv-art-id))))
              (let [virtual-art-id ((first (insert-db :virtual_arts {:identity (id), :virtual_user_id virtual-user-id})) :id)]
                (spit "last.txt" (assoc middle-file1 :last-virtual-art-id virtual-art-id))))
            (let [middle-file1 (read-string (slurp "last.txt"))]
              (insert-db :virtual_pictures {:virtual_user_id virtual-user-id,
                                            :virtual_art_id (middle-file1 :last-virtual-art-id),
                                            :width (Integer/parseInt x-upyun-width),
                                            :height (Integer/parseInt x-upyun-height),
                                            :link link})
              (spit "last.txt" (conj middle-file1 {:last-csv-college-id csv-college-id :last-csv-user-id csv-user-id :last-csv-art-id csv-art-id :last-csv-picture-id csv-picture-id :last-uploadfile path})))
            {:ok {:width x-upyun-width
                  :height x-upyun-height
                  :frames x-upyun-frames
                  :type x-upyun-file-type
                  :url url}}
            )
          {:error (:body result)
           })))
    ))

(defn upload
  [operator_id password path]
  (let [paths (all-file-path path)]
    (doseq [path paths]
      (uploadFile operator_id password path))))

(defn uploadFile+ [operator_id password path]
  (map #(uploadFile operator_id password %) (all-file-path path)))

(defonce server (atom nil))

(defn stop-server []
  (when-not (nil? @server)
    (@server :timeout 100)
    (reset! server nil)))

(defn -main [& args]
  (reset! server (run-server #'app {:port 8080}))
  (prn "Server started!"))
