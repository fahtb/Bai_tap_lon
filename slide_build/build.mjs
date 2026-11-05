import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import sharp from 'sharp';
import {Presentation,PresentationFile} from '@oai/artifact-tool';
const B=path.dirname(fileURLToPath(import.meta.url)), OUT=path.resolve(B,'../SLIDES'), SRC='D:/prj/CINEMA_BIGDATA', TMP='C:/Users/admin/AppData/Local/Temp';
const SK='C:/Users/admin/.codex/plugins/cache/openai-primary-runtime/presentations/26.1029.11837/skills/presentations';
await fs.mkdir(OUT,{recursive:true});
const pres=Presentation.create({slideSize:{width:1600,height:900}}), navy='#173B5F', teal='#087E8B', ink='#182B3A';
const manifest=[];
function text(s,t,x,y,w,h,size=32,color=ink,bold=false,font='Arial'){
const a=s.shapes.add({geometry:'textbox',position:{left:x,top:y,width:w,height:h},fill:'none',line:{fill:'none',width:0}});a.text=[[{run:t,style:{typeface:font,fontSize:size+'px',color,bold}}]];a.text.style={fontFamily:font,fontSize:size,color,bold,verticalAlignment:'top',insets:{left:0,right:0,top:0,bottom:0}};return a;}
function slide(title,notes){const s=pres.slides.add();s.background.fill='#FFFFFF';text(s,title,65,45,1470,95,46,navy,true);text(s,String(pres.slides.items.length).padStart(2,'0'),1470,850,70,30,20,navy);s.speakerNotes.text=notes;manifest.push({slide:pres.slides.items.length,title,sources:[]});return s;}
function source(s,label){manifest.at(-1).sources.push(label);s.speakerNotes.append('\nNguồn minh chứng: '+label);}
function bullets(s,arr,x=65,y=165,w=650,step=110){arr.forEach((a,i)=>text(s,'• '+a,x,y+i*step,w,step-10,32));}
async function crop(id,name,box){let img=sharp(path.join(TMP,'codex-clipboard-'+id+'.png'));if(box)img=img.extract(box);const f=path.join(B,name+'.png');await img.png().toFile(f);return f;}
async function pic(s,file,x,y,w,h,caption){let m=await sharp(file).metadata();let ratio=Math.min(w/m.width,h/m.height);let ww=m.width*ratio,hh=m.height*ratio;s.images.add({blob:new Uint8Array(await fs.readFile(file)),contentType:'image/png',alt:caption,position:{left:x,top:y,width:ww,height:hh},fit:'contain'});text(s,caption,x,y+hh+10,w,55,22,ink);source(s,file);}
function rect(s,x,y,w,h,label){let a=s.shapes.add({geometry:'rect',position:{left:x,top:y,width:w,height:h},fill:'#EDF4F8',line:{fill:navy,width:2}});a.text=[[{run:label,style:{typeface:'Arial',fontSize:'28px',bold:true,color:navy}}]];a.text.style={fontFamily:'Arial',fontSize:28,bold:true,color:navy,alignment:'center',verticalAlignment:'middle'};}
async function code(s,file,start,end,x,y,w,size=24){const lines=(await fs.readFile(path.join(SRC,file),'utf8')).split(/\r?\n/);text(s,`${file} · dòng ${start}–${end}`,x,y,w,40,22,teal,true);let yy=y+42;const max=Math.floor(w/(size*.61))-5;for(let n=start;n<=end;n++){
 let line=lines[n-1]??'';let chunks=[];while(line.length>max){let k=line.lastIndexOf(' ',max);if(k<max*.45)k=max;chunks.push(line.slice(0,k));line='  '+line.slice(k).trimStart();}chunks.push(line);
 for(let j=0;j<chunks.length;j++){const st=chunks[j];const a=text(s,'',x,yy,w,size*1.22,size,ink,false,'Consolas');const runs=[{run:j===0?String(n).padStart(2)+'  ':'    ',style:{typeface:'Consolas',fontSize:size+'px',color:'#637383'}}];for(const tok of st.split(/('[^']*'|"[^"]*"|\b(?:def|return|if|else|for|in|from|import|with|True|False|None|try|except)\b)/g)){runs.push({run:tok,style:{typeface:'Consolas',fontSize:size+'px',color:/^['"]/.test(tok)?teal:/^(def|return|if|else|for|in|from|import|with|True|False|None|try|except)$/.test(tok)?navy:ink,bold:/^(def|return|if|else|for|in|from|import|with)$/.test(tok)}});}a.text=[runs];yy+=size*1.22;}
 }source(s,`${SRC}/${file}, dòng ${start}–${end}`);return yy;}
function table(s,rows,x,y,w,h,size=29){let t=s.tables.add({rows:rows.length,columns:rows[0].length,left:x,top:y,width:w,height:h,values:rows});t.borders.assign({fill:'#CFDCE6',width:1,style:'solid'});for(let r=0;r<rows.length;r++)for(let c=0;c<rows[0].length;c++){let ce=t.getCell(r,c);ce.text.set([[{run:rows[r][c],style:{typeface:'Arial',fontSize:size+'px',color:ink,bold:r===0}}]]);ce.fill=r===0?'#EDF4F8':'#FFFFFF';ce.text.style={fontFamily:'Arial',fontSize:size,color:ink,bold:r===0};}return t;}
const apiO=await crop('d154a066-92c4-4516-b84c-f587b073e137','api-online',{left:240,top:520,width:610,height:240});
const apiC=await crop('3ff17806-0842-4277-bbfb-dea04477f683','api-counter',{left:345,top:471,width:640,height:221});
const ko=await crop('82403548-e02d-49de-9487-045fe8c59c41','kafka-online',{left:1290,top:335,width:600,height:140});
const kc=await crop('3b46d452-5083-4762-a4e8-7f592a82aaa4','kafka-counter',{left:1290,top:335,width:600,height:140});
const cus=await crop('795a5974-5dd9-4b01-9e15-203124272fb0','customers',{left:0,top:30,width:570,height:138});
const rev=await crop('43877718-12c1-4591-b63d-92f1fda7e1fb','revenue',{left:0,top:29,width:420,height:124});
const dag=await crop('4e61a940-e553-4fa4-933c-667d0e9cb6a6','dag',{left:465,top:653,width:1190,height:103});
const docker=await crop('3b716f1b-f1f6-4836-babf-b12ac59c003b','docker',{left:1305,top:43,width:605,height:300});
const checks=await crop('c905e791-36a7-4aed-abb9-b4a3ab248604','checks',{left:15,top:605,width:465,height:151});

let s=slide('Hệ thống dữ liệu dòng\nbán vé chuỗi rạp chiếu phim','Giới thiệu đề tài môn Quản lý dữ liệu lớn. Nhóm gồm Hình Kim Thư và Phạm Thị Xuân Trinh. Giảng viên Nguyễn Thị Hồng Tú. Hai API mô phỏng kênh online và tại quầy. Dữ liệu dùng cho kiểm thử, không phải dữ liệu khách hàng thật.');
text(s,'BÀI TẬP LỚN · QUẢN LÝ DỮ LIỆU LỚN',65,260,1300,60,30,teal,true);
text(s,'Kafka · Spark Structured Streaming · Cassandra\nAirflow · Docker · Windows 11 / WSL2',65,365,1400,120,36);
text(s,'GVHD: Nguyễn Thị Hồng Tú\nSinh viên: Hình Kim Thư — Phạm Thị Xuân Trinh',65,600,1400,110,32);
text(s,'Dữ liệu giả lập có kiểm soát | Minh chứng cập nhật 30/10/2026',65,785,1400,45,25,teal);

s=slide('Bài toán và mục tiêu','Hai kênh bán vé sử dụng schema khác nhau. Mục tiêu là chuẩn hóa, phát hiện lỗi và trùng trước khi tổng hợp doanh thu. Đoạn mã là kiểm tra thực sự trong dự án, không phải mã minh họa. Hệ thống phân tích dữ liệu, không triển khai website đặt vé hoặc cổng thanh toán.');
bullets(s,['Thu thập giao dịch online và tại quầy','Xử lý thiếu trường, sai dữ liệu và phát lại','Tính doanh thu và số vé còn hiệu lực'],65,175,590,150);
await code(s,'producers/normalization.py',27,31,735,185,800,25);

s=slide('Kiến trúc hệ thống','Sơ đồ dựng từ docker-compose.yml và mã producer/Spark thực tế. API online /events và API quầy /sales cung cấp dữ liệu giả lập. Kafka chuyển dữ liệu, Spark xử lý, Cassandra lưu. Airflow điều phối đợt quầy; Spark chạy liên tục ngoài DAG. Docker Compose cung cấp môi trường. Có hai API nhưng chưa có bằng chứng chạy đợt mới toàn tuyến sau khi thay nguồn quầy sang HTTP.');
rect(s,65,180,300,100,'API online /events');rect(s,65,320,300,100,'API quầy /sales');
rect(s,445,250,230,110,'Kafka\n2 topic');rect(s,755,250,320,110,'Spark\nStructured Streaming');rect(s,1155,250,350,110,'Cassandra\nKết quả và bảng lỗi');
text(s,'→',375,265,65,80,48,teal);text(s,'→',685,265,65,80,48,teal);text(s,'→',1085,265,65,80,48,teal);
text(s,'Airflow điều phối tác vụ  •  Docker Compose triển khai dịch vụ',65,465,1440,65,32,navy,true);
await code(s,'spark/streaming_job.py',34,34,65,575,1470,24);source(s,SRC+'/docker-compose.yml');

s=slide('Hai API giả lập bán vé','Bộ demo có 524 bản ghi online và 536 bản ghi quầy theo biên bản docs/evidence/two-apis-validation.md. Ảnh API online chụp trước, ảnh quầy do người dùng cung cấp ngày 30/10. Các phần dưới là crop nguyên ảnh phản hồi, không tái tạo JSON. Ảnh chứng minh API trả dữ liệu, không chứng minh đã gửi vào Kafka hoặc chạy toàn tuyến.');
text(s,'ONLINE · /events · 524 bản ghi',65,165,710,55,32,navy,true);text(s,'QUẦY POS · /sales · 536 bản ghi',830,165,710,55,32,navy,true);
await pic(s,apiO,65,250,710,350,'Ảnh này chứng minh API online trả sự kiện JSON.');await pic(s,apiC,830,250,710,350,'Ảnh này chứng minh API quầy trả giao dịch JSON.');
text(s,'Hai định dạng nguồn → cùng mô hình sự kiện',65,695,1450,60,36,teal,true);text(s,'HTTP trả dữ liệu ≠ kiểm thử toàn tuyến hai API',65,775,1450,45,30);
source(s,SRC+'/docs/evidence/two-apis-validation.md');

s=slide('Thu thập và truyền qua Kafka','Hai ảnh topic do người dùng cung cấp, mỗi ảnh có tên topic và bản tin. Producer online gọi HTTP theo trang 500 và gửi ticket_online; producer quầy gọi /sales và gửi ticket_counter. Khóa bản tin là suất chiếu. Không dùng ảnh Kafka để suy ra bản tin của một lần gọi HTTP vừa thực hiện. Mã trích giữ nguyên, chỉ xuống dòng để đọc.');
text(s,'Topic ticket_online',65,155,710,45,30,navy,true);text(s,'Topic ticket_counter',830,155,710,45,30,navy,true);
await pic(s,ko,65,215,710,185,'Ảnh này chứng minh topic ticket_online có bản tin.');await pic(s,kc,830,215,710,185,'Ảnh này chứng minh topic ticket_counter có bản tin.');
await code(s,'producers/producer_online.py',13,18,65,470,1470,25);source(s,SRC+'/producers/producer_counter.py');

s=slide('Chuẩn hóa và xử lý bằng Spark','Chuẩn hóa tên trường POS, đổi Amount có dấu chấm thành số nguyên, parse TimeSold UTC+7 thành UTC. Join danh mục suất chiếu, phòng, phim để bổ sung movie_id và seat_count. Nhánh hợp lệ có watermark 10 phút và khử trùng theo event_id. Đoạn code là nguyên dòng 81 của streaming_job.py, được wrap. Ví dụ Amount 95.000 và TimeSold 25/10/2026 14:00:00 lấy từ ảnh API quầy; kết quả chuyển đổi theo normalization.py.');
bullets(s,['Chuẩn hóa tên trường, giá và thời gian','Kiểm tra lỗi; liên kết danh mục suất chiếu','Khử trùng sự kiện trong trạng thái watermark'],65,155,1470,75);
table(s,[['Đầu vào POS','Sau chuẩn hóa'],['Amount: "95.000"','price: 95000'],['25/10/2026 14:00:00 (UTC+7)','2026-10-25 07:00:00 UTC']],65,405,1470,180,30);
await code(s,'spark/streaming_job.py',81,81,65,645,1470,26);source(s,SRC+'/producers/normalization.py, dòng 5–19');

s=slide('Quy tắc tính doanh thu và số vé','Mã latest_paid lấy trạng thái mới nhất theo thời gian và ưu tiên khi cùng thời điểm. Doanh thu quy về giờ thanh toán gốc, nếu trạng thái cuối CANCELLED thì tickets/revenue bằng 0. Ví dụ khách U000000 và vé demo-T000000000 có BOOKED 06:59:50Z, PAID 07:00:00Z, CANCELLED 07:00:10Z, giá 95000 theo docs/evidence/ui-validation.md. Mô hình chưa hoàn tiền từng phần hoặc giải phóng ghế sau hủy.');
table(s,[['06:59:50 UTC','07:00:00 UTC','07:00:10 UTC'],['BOOKED','PAID · 95.000 đ','CANCELLED']],65,170,1470,150,32);
text(s,'Vé demo-T000000000: trạng thái cuối hủy → đóng góp 0 vé, 0 đồng',65,365,1470,90,32,teal,true);
await code(s,'spark/aggregates.py',10,10,65,510,1470,25);source(s,SRC+'/docs/evidence/ui-validation.md');

s=slide('Lưu trữ và truy vấn Cassandra','Hai ảnh truy vấn của người dùng sau chuyển sang ổ D. Bảng customers là hồ sơ giả lập, không phải người thật. Doanh thu bốn nhóm cộng thành 91.050.000 đồng, 940 vé. Các bảng ledger/lỗi/doanh thu có khóa phục vụ truy vấn; đoạn CQL tạo bảng doanh thu lấy nguyên từ init.cql.');
await pic(s,cus,65,165,740,235,'Ảnh này chứng minh bảng customers có dữ liệu.');await pic(s,rev,855,165,670,235,'Ảnh này chứng minh doanh thu được lưu theo kênh/giờ.');
text(s,'Sự kiện → ticket_events   |   Lỗi → invalid_events',65,505,1470,55,32);
await code(s,'cassandra/init.cql',5,5,65,610,1470,25);

s=slide('Điều phối bằng Airflow','Ảnh mới thể hiện đúng sáu task trong DAG. Ảnh không chứng minh toàn bộ sáu task thành công hay retry đã thử. Lịch @hourly, retries=2, retry_delay 30 giây là cấu hình trong mã. Lần kiểm thử CLI cũ là năm task, sent=0; không dùng duration hơn bốn ngày trên UI làm chỉ số hiệu năng.');
await pic(s,dag,65,165,1470,150,'Ảnh này chứng minh DAG hiện tại có sáu task và chuỗi phụ thuộc.');
await code(s,'airflow/dags/ticket_pipeline_hourly.py',58,61,65,395,1470,26);
text(s,'Đã có cấu hình retry; chưa có minh chứng thử lỗi rồi retry thành công.',65,700,1470,85,32,teal,true);

s=slide('Triển khai bằng Docker Compose','Ảnh là phần service và status crop từ docker compose ps người dùng cung cấp ngày 30/10 sau khi sao chép sang ổ D. Không có chỉnh sửa nội dung log. Dịch vụ có healthcheck hiển thị healthy; scheduler và UI chỉ Up do không cấu hình healthcheck. Thư mục làm việc /mnt/d/prj/CINEMA_BIGDATA. Khởi động lại bằng script open_demo.sh, không phát lại producer. Dữ liệu Kafka/Cassandra/checkpoint ở named volumes, không xóa volume.');
bullets(s,['Windows 11 + Ubuntu WSL2','Dự án: /mnt/d/prj/CINEMA_BIGDATA','Giữ dữ liệu và checkpoint khi mở lại'],65,155,650,105);
await pic(s,docker,800,160,735,385,'Ảnh này chứng minh các dịch vụ đang chạy tại lần chụp.');
await code(s,'docker-compose.yml',127,133,65,530,1470,24);

s=slide('Kết quả kiểm thử trên bộ demo','980 sự kiện hợp lệ không có nghĩa là 980 vé bán được, vì một vé có thể có nhiều sự kiện đặt, thanh toán, hủy. Ảnh ngày 30/10 là lệnh không baseline nên có 8/8 checks. Bằng chứng khôi phục cũ recovery_after.json có 10/10 bao gồm hai kiểm tra trước/sau. 10/10 chỉ áp dụng kịch bản sau batch commit, không khẳng định exactly once mọi tình huống. Cả hai nguồn API mới đã được kiểm tra HTTP, chưa chứng minh đợt mới đi toàn tuyến.');
text(s,'1.060 bản ghi → 980 sự kiện hợp lệ → 940 vé hiệu lực',65,150,1470,70,35,navy,true);
text(s,'Doanh thu: 91.050.000 đồng',65,235,1470,60,40,teal,true);
await pic(s,checks,65,350,750,260,'Ảnh này chứng minh 8/8 đối soát hiện tại đạt ngày 30/10.');
table(s,[['Lỗi','Số'],['MISSING_FIELD','20'],['BAD_PRICE','10'],['BAD_TIME','10'],['DOUBLE_BOOKING','5'],['FUTURE','5']],875,340,660,355,28);
text(s,'Khôi phục trước đây: 10/10 kiểm tra đạt trong kịch bản đã thử.',65,760,1470,55,30);
source(s,SRC+'/docs/evidence/recovery_after.json');source(s,SRC+'/data/slide_check.json');

s=slide('Hiệu năng và giới hạn đo lường','Số đo trích trực tiếp durationMs.triggerExecution từ ba dòng progress.jsonl đã lưu. Đơn vị giây đổi từ ms: 20822/1000, 26695/1000, 1113/1000. Hai query song song không cộng thời gian. numInputRows=2120 của kế hoạch recompute không tương đương số vé hoặc số bản tin nghiệp vụ 1060. Chưa đo p95, RAM/CPU tải dài hoặc benchmark nhiều mức. Đoạn mã chứng minh đang đọc lại ledger mỗi lần refresh.');
table(s,[['Truy vấn','Batch','Thời gian trigger'],['audit','0','20,822 giây'],['valid','0','26,695 giây'],['valid','1 · không có dữ liệu','1,113 giây']],65,160,1470,250,30);
text(s,'Nguồn số đo: data/metrics/progress.jsonl · dòng 1–3',65,430,1470,40,22,teal);
await code(s,'spark/aggregates.py',15,15,65,500,1470,24);
text(s,'Một máy; quét lại ledger. Chưa có benchmark tải dài hoặc p95.',65,730,1470,70,32);
source(s,SRC+'/data/metrics/progress.jsonl, dòng 1–3');

s=slide('Kết luận và hướng phát triển','Các số liệu đạt được thuộc fixture demo và đối soát đã lưu. Hướng ưu tiên là thử toàn tuyến hai API bằng run mới, thử retry Airflow, cải tiến tổng hợp tăng dần, sau đó kiểm tra nhiều broker/node. Trích dẫn mã hai topic cho thấy phần kết nối đã có, nhưng không thay thế kiểm thử tích hợp. Không nhận đã đạt điểm tối đa.');
bullets(s,['Đã có luồng xử lý và kết quả demo đối soát được','Hai API đã trả dữ liệu; cần thử đợt mới toàn tuyến','Cần thử retry Airflow và lỗi giữa quá trình ghi','Hướng tới tổng hợp tăng dần và kiểm thử mở rộng'],65,160,1470,105);
await code(s,'producers/producer_counter.py',27,28,65,625,1470,25);

s=slide('Phân công nhóm và hỏi đáp','Phần công việc được để trống theo yêu cầu người dùng, không suy đoán đóng góp. Người trình bày tự điền trước khi nộp. Gợi ý vấn đáp: vì sao 980 sự kiện không phải 980 vé; watermark quản lý gì; hai topic không đủ chứng minh hai API; checkpoint chưa bảo đảm ghi nguyên tử nhiều sink.');
table(s,[['Thành viên','Công việc thực tế'],['Hình Kim Thư',''],['Phạm Thị Xuân Trinh','']],65,190,1470,260,32);
text(s,'Cảm ơn cô và các bạn',65,580,1470,80,46,navy,true);text(s,'Mời đặt câu hỏi',65,695,1470,60,36,teal);

await fs.writeFile(path.join(B,'evidence_manifest.json'),JSON.stringify(manifest,null,2));
await (await PresentationFile.exportPptx(pres)).save(path.join(B,'candidate.pptx'));
for(let i=0;i<pres.slides.items.length;i++){let blob=await pres.export({slide:pres.slides.items[i],format:'png',scale:1});await fs.writeFile(path.join(B,`slide-${i+1}.png`),new Uint8Array(await blob.arrayBuffer()));}
console.log('DRAFT_RENDERED');
const {finalizePresentation}=await import(pathToFileURL(path.join(SK,'container_tools/artifact_tool_utils.mjs')).href);
console.log(await finalizePresentation({workspaceDir:path.resolve(B,'../..'),candidatePath:path.join(B,'candidate.pptx'),finalPath:path.join(OUT,'Slide_Quan_ly_du_lieu_ban_ve_rap_phim.pptx'),pythonExecutable:'C:/Users/admin/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe',integrityValidatorPath:path.join(SK,'container_tools/inspect_presentation_package_integrity.py'),layoutValidatorPath:path.join(SK,'container_tools/inspect_presentation_layout_geometry.py'),layoutArgs:['--expected-slide-size-emu','15240000,8572500',...[6,7,11,12,14].flatMap(n=>['--require-native-table-slide',String(n)])],explicitTotalSlideCount:14,requiredNativeTableOwnerSlides:[6,7,11,12,14],fontPolicy:{basis:'design',families:['Arial','Consolas']},verifyArtifactToolImport:true,receiptPath:path.join(B,'validation.json')}));
